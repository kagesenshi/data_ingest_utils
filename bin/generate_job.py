#!/usr/bin/env python

import sys
import json
import argparse
from collections import OrderedDict
import os
import shutil
from datetime import datetime

hive_create_template = '''
drop table if exists staging.%(source_name)s_%(schema)s_%(table)s;
create external table staging.%(source_name)s_%(schema)s_%(table)s (
   %(columns_create_newline)s
)            
STORED AS PARQUET
LOCATION '/user/trace/source/%(source_name)s/%(schema)s_%(table)s/CURRENT';
'''

falcon_process_template = (
'''<process xmlns='uri:falcon:process:0.1' name="%(process_name)s">
    <tags>entity_type=process,activity_type=ingestion,stage=%(stage)s,source=%(source_name)s,schema=%(schema)s,table=%(table)s,ingest_type=%(ingest_type)s</tags>
    <clusters>
        <cluster name="TMDATALAKEP">
            <validity start="%(start_utc)s" end="2099-12-31T00:00Z"/>
        </cluster>
    </clusters>
    <parallel>1</parallel>
    <order>FIFO</order>
    <frequency>hours(%(frequency_hours)s)</frequency>
    <timezone>GMT+08:00</timezone>
    <outputs>
        %(outputs)s
    </outputs>
    <properties>
       %(properties)s
    </properties>
    <workflow engine="oozie" path="%(workflow_path)s"/>
    <retry policy='periodic' delay='minutes(30)' attempts='3'/>
</process>
''')

falcon_feed_template = '''
<feed xmlns='uri:falcon:feed:0.1' name='%(feed_name)s'>
  <tags>entity_type=feed,format=%(feed_format)s,stage=%(stage)s,source=%(source_name)s,schema=%(schema)s,table=%(table)s,ingest_type=%(ingest_type)s</tags>
  <availabilityFlag>_SUCCESS</availabilityFlag>
  <frequency>days(1)</frequency>
  <timezone>GMT+08:00</timezone>
  <late-arrival cut-off='hours(4)'/>
  <clusters>
    <cluster name='TMDATALAKEP' type='source'>
      <validity start='%(start_utc)s' end='2099-12-31T00:00Z'/>
      <retention limit='days(1825)' action='delete'/>
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
  <ACL owner='admin' group='users' permission='0x755'/>
  <schema location='/none' provider='/none'/>
  <properties>
    <property name='queueName' value='oozie'></property>
    <property name='jobPriority' value='NORMAL'></property>
  </properties>
</feed>
'''


oozie_properties = OrderedDict([
    ('resourceManager','hdpmaster1.tm.com.my:8050'),
    ('jobTracker','hdpmaster1.tm.com.my:8050'),
    ('nameNode','hdfs://hdpmaster1.tm.com.my:8020'),
    ('oozie.wf.application.path','/user/trace/workflows/%(workflow)s/'),
    ('oozie.use.system.libpath','true'),
    ('user.name','trace'),
    ('mapreduce.job.user.name','trace'),
    ('oozie.launcher.mapreduce.job.queue.name', 'oozie'),
    ('mapred.job.queue.name', 'ingestion'),
    ('queueName','ingestion'),
    ('prefix', None),
    ('jdbc_uri','jdbc:oracle:thin:@%(host)s:%(port)s/%(tns)s'),
    ('username', None),
    ('password',None),
    ('source_name', None),
    ('direct', None),
    ('targetdb', None),
    ('stagingdb', None),
    ('schema', None),
    ('table', None),
    ('mapper', None),
    ('split_by', None),
    ('merge_column', None),
    ('check_column', None),
    ('field_delimiter', '~^'),
    ('columns', None),
    ('columns_create', None),
    ('columns_java', None),
    ('columns_flat', None),
])

TYPE_MAP = {
    'VARCHAR2': 'STRING',
    'DATE': 'STRING',
    'NUMBER': 'STRING',
    'CHAR': 'STRING',
    'LONG': 'BIGINT',
    'CLOB': 'BINARY',
    'RAW': 'BINARY'
}

JAVA_TYPE_MAP = {
    'VARCHAR2': 'String',
    'DATE': 'String',
    'NUMBER': 'String',
    'CHAR': 'String',
    'LONG': 'Long',
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

FEED_TYPE = {
   'source': {
       'path': '%(prefix)s/source/%(source_name)s/%(schema)s_%(table)s/ingest_date=${YEAR}-${MONTH}-${DAY}',
       'format': 'parquet',
   },
   'source_flat': {
        'path': '%(prefix)s/source_flat/%(source_name)s/%(schema)s_%(table)s/${YEAR}-${MONTH}-${DAY}',
        'format': 'flat',
   },
   'source_increment': {
        'path': '%(prefix)s/source_increment/%(source_name)s/%(schema)s_%(table)s/increment_date=${YEAR}-${MONTH}-${DAY}',
        'format': 'parquet',
   },
   'source_increment_flat': {
        'path': '%(prefix)s/source_increment_flat/%(source_name)s/%(schema)s_%(table)s/${YEAR}-${MONTH}-${DAY}',
        'format': 'flat'
   },
}

def oozie_config(properties):
    prop = oozie_properties.copy()
    prop['jdbc_uri'] = prop['jdbc_uri'] % properties
    prop['oozie.wf.application.path'] = properties['wfpath']
    for k,v in prop.items():
        if k in properties.keys():
            prop[k] = properties[k]
    return prop

def falcon_process(stage, properties):
    prop = oozie_config(properties)
    outputs = []
    for feed_type in ['source', 'source_flat']:
        feed_name = (
          stage + '-%(source_name)s-%(schema)s-%(table)s-' % properties +
          feed_type
        ).replace('_','-')
        outputs.append(feed_name)
    if 'increment' in properties['workflow']:
        for feed_type in ['source_increment', 'source_increment_flat']:
           feed_name = (
              stage + '-%(source_name)s-%(schema)s-%(table)s-' % properties +
              feed_type
           ).replace('_','-')
           outputs.append(feed_name)
    params = {
        'schema': properties['schema'],
        'table': properties['table'],
        'ingest_type': properties['workflow'],
        'source_name': properties['source_name'],
        'start_utc': datetime.utcnow().strftime('%Y-%m-%dT19:00Z'),
        'should_end_hours': 5,
        'frequency_hours': 24,
        'process_name': (stage + '-%(source_name)s-%(schema)s-%(workflow)s' % properties).replace('_',''),
        'workflow_path': prop['oozie.wf.application.path'],
        'stage' : stage,
        'properties': '\n       '.join(
            ['<property name="%s" value="%s"/>' % (k,v) for (k,v) in prop.items() if '.' not in k]),
        'outputs': '\n        '.join(
            ['<output name="output%s" feed="%s" instance="today(0,0)"/>' % (t,i) for t,i in enumerate(outputs)])
    }
    job = falcon_process_template % params
    return job

def falcon_feed(stage, properties):
    params = {
       'schema': properties['schema'],
       'table': properties['table'],
       'source_name': properties['source_name'],
       'start_utc': datetime.utcnow().strftime('%Y-%m-%dT22:00Z'),
       'ingest_type': properties['workflow'],
       'feed_name': (
           stage + '-%(source_name)s-%(schema)s-%(table)s-%(feed_type)s' % properties
       ).replace('_','-'),
       'feed_path': properties['feed_path'],
       'feed_type': properties['feed_type'],
       'feed_format': properties['feed_format'],
       'stage': stage
    }
    job = falcon_feed_template % params
    return job

def main():
    argparser = argparse.ArgumentParser(description='Generate oozie and falcon configurations for ingestion')
    argparser.add_argument('profilerjson', help='JSON output from oracle_profiler.py')
    opts = argparser.parse_args()
    hive_create = []
    if os.path.exists('artifacts/'):
        shutil.rmtree('artifacts/')
    for ds in json.loads(open(opts.profilerjson).read()):
        for table in ds['tables']:
            mapper = int((table['estimated_size'] or 0) / 1024 / 1024 / 1024) or 2
            if mapper > 20:
                mapper = 20
            columns = [c['field'] for c in table['columns']]
            columns_create = []
            columns_java = []
            columns_flat = []
            for c in table['columns']:
                columns_create.append('`%s` %s' % (c['field'] , TYPE_MAP[c['type']]))
                if JAVA_TYPE_MAP.get(c['type'], None):
                    columns_java.append('%s=%s' % (c['field'], JAVA_TYPE_MAP[c['type']]))

                if TYPE_MAP[c['type']] == 'STRING':
                    columns_flat.append("regexp_replace(`%s`,'\\\\n',' ') AS `%s`" % (c['field'],c['field']))
                else:
                    columns_flat.append("`%s`" % c['field'])

            source_name = ds['datasource']['name'].replace(' ','_')
            username = ds['datasource']['login']
            password = ds['datasource']['password']
   
            if table['merge_key'] and table['check_column']:
                workflow = 'incremental-ingest'
            else:
                if ds['direct']:
                    workflow = 'full-ingest'
                else:
                    workflow = 'full-ingest-nodirect'
            params = {
                'mapper': mapper, 
                'source_name': source_name,
                'host': ds['datasource']['ip'],
                'port': ds['datasource']['port'],
                'username': username, # ds['datasource']['login'],
                'password': password,
                'tns': ds['datasource']['tns'],
                'schema': ds['datasource']['schema'],
                'table': table['table'],
                'split_by': table['split_by'],
                'columns_java': ','.join(columns_java),
                'columns_create': ','.join(columns_create),
                'columns_create_newline': ',\n    '.join(columns_create),
                'columns_flat': ','.join(columns_flat),
                'columns': ','.join([c['field'] for c in table['columns']]),
                'workflow': workflow,
                'merge_column': table['merge_key'],
                'check_column': table['check_column'],
                'direct': ds['direct']
            }
   
            if params['source_name'] == 'CPC' and params['schema'] == 'SIEBEL' and params['table'] == 'S_CONTACT':
                params['field_delimiter'] = '^~'


            for stage, conf in STAGES.items():
                for ingest in ['full-ingest', 'full-ingest-sqooponly', 'incremental-ingest', 'incremental-ingest-frozen']:
                    opts = params.copy()

                    # Oozie
                  
                    ingest_wf = ingest
                    if not opts['direct'] and ingest == 'full-ingest':
                       ingest_wf = ingest + '-nodirect'

                    opts['wfpath'] = os.path.join(conf['prefix'],'workflows', ingest_wf)
                    opts['prefix'] = conf['prefix']
                    opts['targetdb'] = conf['targetdb'] % params
                    opts['stagingdb'] = conf['stagingdb'] 

                    filename = '%(source_name)s-%(schema)s-%(table)s.properties' % opts
                    storedir = 'artifacts/%s-oozie-%s' % (stage, ingest)

                    # sources without merge_column and check_column cant be ingested incremental
                    if 'incremental' in ingest and not (opts['merge_column'] and opts['check_column']):
                        continue

                    # use only 1 mapper for small tables when incremental
                    if 'increment' in ingest:
                       if opts['mapper'] < 5:
                          opts['mapper'] = 1
                       elif opts['mapper'] > 10:
                          opts['mapper'] = 10

                    if not os.path.exists(storedir):
                        os.makedirs(storedir)
                    with open('%s/%s' % (storedir, filename), 'w') as f:
                        job = '\n'.join(['%s=%s' % (k,v) for k,v in oozie_config(opts).items()])
                        f.write(job)

                    # Falcon Process
                    # sources that can be ingested incrementally does not need falcon full-ingest entity
                    if 'full-ingest' in ingest and (opts['merge_column'] and opts['check_column']):
                        continue

                    filename = '%(source_name)s-%(schema)s-%(table)s.xml' % opts
                    storedir = 'artifacts/%s-falconprocess-%s' % (stage, ingest)
                    if not os.path.exists(storedir): 
                        os.makedirs(storedir)
                    with open('%s/%s' % (storedir,filename), 'w') as f:
                        job = falcon_process(stage, opts)
                        f.write(job)

                for feed_type, feed_info in FEED_TYPE.items():
                    opts = params.copy()
                    if 'increment' in feed_type and not (opts['merge_column'] and opts['check_column']):
                        continue
                    opts['prefix'] = conf['prefix']
                    opts['targetdb'] = conf['targetdb'] % params
                    opts['stagingdb'] = conf['stagingdb'] 

                    opts['feed_type'] = feed_type
                    opts['feed_format'] = feed_info['format']

                    opts['feed_path'] = feed_info['path'] % opts
                    filename = '%(source_name)s-%(schema)s-%(table)s-%(feed_type)s.xml' % opts
                    storedir = 'artifacts/%s-falconfeed' % (stage)
                    if not os.path.exists(storedir): 
                        os.makedirs(storedir)
                    with open('%s/%s' % (storedir,filename), 'w') as f:
                        job = falcon_feed(stage, opts)
                        f.write(job)

    
            # generate hive create table
            hive_create.append(hive_create_template % params) 
    
    with open('external-tables.sql', 'w') as f:
         f.write('\n\n'.join(hive_create))

if __name__ == '__main__':
    main()
