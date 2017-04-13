#!/usr/bin/env python

import sys
import json
import argparse
from collections import OrderedDict
import os
import shutil
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
import csv

falcon_process_template = (
'''<process xmlns='uri:falcon:process:0.1' name="%(process_name)s">
    <tags>entity_type=process,activity_type=ingestion,stage=%(stage)s,source=%(source_name)s,schema=%(schema)s,table=%(table)s,workflow=%(workflow)s</tags>
    <clusters>
        <cluster name="TMDATALAKEP">
            <validity start="%(start_utc)s" end="2099-12-31T00:00Z"/>
        </cluster>
    </clusters>
    <parallel>1</parallel>
    <order>FIFO</order>
    <frequency>hours(%(frequency_hours)s)</frequency>
    <timezone>GMT+08:00</timezone>
    %(inputs)s
    %(outputs)s
    <properties>
        %(properties)s
        <property name="oozie.processing.timezone" value="UTC" />
    </properties>
    <workflow name="%(workflow_name)s" engine="oozie" path="%(workflow_path)s"/>
    <retry policy='periodic' delay='minutes(30)' attempts='3'/>
</process>
''')

falcon_feed_template = '''
<feed xmlns='uri:falcon:feed:0.1' name='%(feed_name)s'>
  <tags>entity_type=feed,format=%(feed_format)s,stage=%(stage)s,source=%(source_name)s,schema=%(schema)s,table=%(table)s,feed_type=%(feed_type)s</tags>
  <availabilityFlag>_SUCCESS</availabilityFlag>
  <frequency>days(1)</frequency>
  <timezone>GMT+08:00</timezone>
  <late-arrival cut-off='hours(18)'/>
  <clusters>
    <cluster name='TMDATALAKEP' type='source'>
      <validity start='%(start_utc)s' end='2099-12-31T00:00Z'/>
      %(retention)s
      <locations>
        <location type='data' path='%(feed_path)s'></location>
        <location type='stats' path='/'></location>
      </locations>
    </cluster>
  </clusters>
  <locations>
    <location type='data' path='%(feed_path)s'></location>
    <location type='stats' path='/'></location>
  </locations>
  <ACL owner='trace' group='users' permission='0x755'/>
  <schema location='/none' provider='/none'/>
  <properties>
    <property name='queueName' value='oozie'></property>
    <property name='jobPriority' value='NORMAL'></property>
    <property name="oozie.processing.timezone" value="UTC" />
  </properties>
</feed>
'''



oozie_properties = OrderedDict([
    ('resourceManager','hdpmaster1.tm.com.my:8050'),
    ('jobTracker','hdpmaster1.tm.com.my:8050'),
    ('nameNode','hdfs://hdpmaster1.tm.com.my:8020'),
    ('oozie.wf.application.path','/user/trace/workflows/%(workflow)s/'),
    ('oozie.use.system.libpath','true'),
    ('prefix', None),
    ('source_name', None),
    ('targetdb', None),
    ('stagingdb', None),
    ('schema', None),
    ('table', None),
    ('merge_column', None),
    ('check_column', None),
    ('reconcile', 'merge'),
])

JAVA_TYPE_MAP = {
#    'VARCHAR2': 'String',
    'DATE': 'String',
#    'NUMBER': 'String',
#    'CHAR': 'String',
#    'LONG': 'Long',
}

STAGES = {
   'dev': {
      'prefix': '/user/trace/development/',
      'targetdb': '%(source_name)s_DEV',
      'stagingdb': 'staging_dev',
   },
   'test': {
      'prefix': '/user/trace/test/',
      'targetdb': '%(source_name)s',
      'stagingdb': 'staging_test',
   },
   'prod': {
      'prefix': '/user/trace/',
      'targetdb': '%(source_name)s',
      'stagingdb': 'staging',
   }
}

PROCESSES = {
    'transform-files': {
        'workflow': 'transform-files',
    },
    'transform-files-full': {
        'workflow': 'transform-files-full'
    },
    'ingest-files-raw-current': {
        'workflow': 'ingest-files-raw-current'
    },
}

EXEC_TIME = {
    'EAI': {
        'ingest-full': '00:01',
        'ingest-increment': '00:01',
        'transform-full': '02:00',
        'transform-increment': '02:00'
    },
    'SIEBEL_NOVA': {
        'ingest-full': '03:00',
        'ingest-increment': '03:00',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'BRM_NOVA': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'GRANITE': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'NIS': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    },
    'PORTAL': {
        'ingest-full': '03:01',
        'ingest-increment': '03:01',
        'transform-full': '05:00',
        'transform-increment': '05:00'
    }
}

FEEDS = {
   'full-retention': {
       'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/instance_date=${YEAR}-${MONTH}-${DAY}',
       'format': 'parquet',
       'exec_time': '00:00',
       'retention': 365
   },
#   'increment-retention': {
#        'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/INCREMENT/instance_date=${YEAR}-${MONTH}-${DAY}',
#        'format': 'parquet',
#        'exec_time': '00:00',
#        'retention': 365
#   },
   'full': {
       'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/CURRENT/',
       'format': 'parquet',
       'exec_time': '00:00',
   },
#   'increment': {
#        'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/INCREMENT/CURRENT',
#        'format': 'parquet',
#        'exec_time': '00:00'
#   },
}

ARTIFACTS='files-artifacts/'

FOREVER=36135

def get_exec_time(source, process):
    return EXEC_TIME.get(source, {}).get(process, '03:01')

def generate_utc_time(t, dayoffset=0):
    tt = (datetime.now() + timedelta(days=dayoffset)).strftime('%Y-%m-%d')
    dt = tt + ' %s' % t
    start_dt = parse_date(dt)
    return (start_dt - timedelta(hours=8)).strftime('%Y-%m-%dT%H:%MZ')

def default_feed_name(stage, properties, feed):
    return (stage + 
        '-%(source_name)s-%(schema)s-%(table)s-' % properties +
        feed
    ).replace('_','')

def default_process_name(stage, properties):
    return (
        stage + 
        '-%(source_name)s-%(schema)s-%(table)s-%(workflow)s' % properties
    ).replace('_','')

def write_oozie_config(storedir, properties):
    filename = '%(source_name)s-%(schema)s-%(table)s.properties' % properties
    if not os.path.exists(storedir):
        os.makedirs(storedir)
    with open('%s/%s' % (storedir, filename), 'w') as f:
        job = '\n'.join([
            '%s=%s' % (k,v) for k,v in oozie_config(properties).items()
        ])
        f.write(job)

def oozie_config(properties):
    prop = oozie_properties.copy()
    prop['oozie.wf.application.path'] = properties['wfpath']
    for k,v in prop.items():
        if k in properties.keys():
            prop[k] = properties[k]
    prop['appName'] = (
        properties.get('appName', None) or 
        '%(workflow)s-%(source_name)s-%(schema)s-%(table)s' % properties
    )
    return prop

def write_falcon_process(storedir, stage, properties, in_feeds=None,
        out_feeds=None, process_time='05:00'):
    filename = '%(source_name)s-%(schema)s-%(table)s.xml' % properties
    if not os.path.exists(storedir):
        os.makedirs(storedir)
    with open('%s/%s' % (storedir, filename), 'w') as f:
        params, job = falcon_process(stage, properties, in_feeds, out_feeds,
                process_time)
        f.write(job)

   

def falcon_process(stage, properties, in_feeds=None, out_feeds=None,
        process_time='05:00'):
    inputs = [
        default_feed_name(stage, properties, 
            i) for  i in (in_feeds or [])
        ]
    outputs = [
        default_feed_name(stage, properties, 
            i) for  i in (out_feeds or [])
        ]
    inputs_xml = '\n        '.join(
            ['<input name="input%s" feed="%s" start="today(8,0)" end="today(9,30)"/>' % 
                (t,i) for t,i in enumerate(inputs)])
    inputs_xml = '<inputs>%s</inputs>' % inputs_xml if inputs_xml else ''
    outputs_xml = '\n        '.join(
            ['<output name="output%s" feed="%s" instance="today(8,0)"/>' %
                (t,i) for t,i in enumerate(outputs)])
    outputs_xml = '<outputs>%s</outputs>' % outputs_xml if outputs_xml else ''

    prop = oozie_config(properties)
    params = {
        'schema': properties['schema'],
        'table': properties['table'],
        'workflow': properties['workflow'],
        'source_name': properties['source_name'],
        'start_utc': generate_utc_time(process_time, dayoffset=1),
        'should_end_hours': 5,
        'frequency_hours': 24,
        'process_name': default_process_name(stage, properties),
        'workflow_path': prop['oozie.wf.application.path'],
        'workflow_name': prop['appName'].replace('_',''),
        'stage' : stage,
        'properties': '\n       '.join(
            ['<property name="%s" value="%s"/>' % (k,v) for (k,v) in prop.items() if '.' not in k]),
        'outputs': outputs_xml,
        'inputs': inputs_xml,
    }
    job = falcon_process_template % params
    return params, job


def write_falcon_feed(storedir, stage, properties, feed, feed_path, 
        feed_format, exec_time='00:00', retention=FOREVER):
    filename = '%(source_name)s-%(schema)s-%(table)s.xml' % properties
    if not os.path.exists(storedir):
        os.makedirs(storedir)
    with open('%s/%s' % (storedir, filename), 'w') as f:
        params, job = falcon_feed(stage, properties, feed, 
                    feed_path, feed_format, exec_time, retention)
        f.write(job)

def falcon_feed(stage, properties, feed, feed_path, feed_format, 
            exec_time='00:00', retention=FOREVER):
    if retention is not None:
        rt = "<retention limit='days(%s)' action='delete'/>" % retention
    else:
        rt = ''
    params = {
       'schema': properties['schema'],
       'table': properties['table'],
       'source_name': properties['source_name'],
       'start_utc': generate_utc_time(exec_time),
       'feed_name': default_feed_name(stage, properties, feed), 
       'feed_path': feed_path % properties,
       'feed_type': feed,
       'feed_format': feed_format,
       'stage': stage,
       'retention': rt
    }
    job = falcon_feed_template % params
    return params, job

def main():
    argparser = argparse.ArgumentParser(description='Generate oozie and falcon configurations for ingestion')
    argparser.add_argument('tabletsv', help='TSV of the table list')
    opts = argparser.parse_args()
    if os.path.exists(ARTIFACTS):
        shutil.rmtree(ARTIFACTS)
    for table in csv.DictReader(open(opts.tabletsv), delimiter='\t'):
            params = {
                'source_name': table['Datasource'],
                'schema': table['Schema'],
                'table': table['Table'],
                'merge_column': table['MergeKey'],
                'check_column': table['CheckColumn'],
                'reconcile': 'merge' if table['MergeKey'] else 'append'
            }
   
            for stage, conf in STAGES.items():

                if stage in ['prod']:
                    opts = params.copy()
                    opts['stagingdb'] = conf['stagingdb']
                    opts['targetdb'] = conf['targetdb'] % params
                    opts['prefix'] = conf['prefix']
    
                for process, proc_opts in PROCESSES.items():
                    opts = params.copy()
                    opts['prefix'] = conf['prefix']
                    opts['targetdb'] = conf['targetdb'] % params
                    opts['stagingdb'] = conf['stagingdb'] 

                    wf = proc_opts['workflow']
                    opts['workflow'] = wf
                    opts['wfpath'] = os.path.join(conf['prefix'],'workflows', wf)
                    if not proc_opts.get('condition', lambda x: True):
                        continue

                    storedir = '%s/%s-oozie-%s' % (ARTIFACTS, stage, wf)
                    write_oozie_config(storedir, opts)
                    storedir = '%s/%s-falconprocess-%s' % (ARTIFACTS, stage, wf)
                    write_falcon_process(storedir, stage, opts,
                        proc_opts.get('in_feeds', []), 
                        proc_opts.get('out_feeds', []),
                        get_exec_time(opts['source_name'], process)
                    )

                for feed, feed_opts in FEEDS.items():
                    opts = params.copy()
                    opts['prefix'] = conf['prefix']
                    storedir = '%s/%s-falconfeed-%s' % (ARTIFACTS, stage, feed)
                    write_falcon_feed(storedir, stage, opts, feed,
                                    feed_opts['path'], feed_opts['format'],
                                    feed_opts['exec_time'],
                                    feed_opts.get('retention', FOREVER))

if __name__ == '__main__':
    main()
