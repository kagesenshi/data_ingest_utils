import sys
import json

properties = {
    'resourceManager': 'hdpmaster1.tm.com.my:8050',
    'queueName': 'ingestion',
    'nameNode': 'hdfs://hdpmaster1.tm.com.my:8020',
    'mapper': None,
    'source_name': None,
    'jdbc_uri': 'jdbc:oracle:thin:@%(host)s:%(port)s/%(tns)s',
    'schema': None,
    'table': None,
    'split_by': None,
    'username': None,
    'password': None,
    'oozie.wf.application.path': '/user/trace/workflows/full-ingest/',
    'oozie.use.system.libpath': 'true',
    'user.name': 'trace',
    'mapreduce.job.user.name': 'trace',
    'jobTracker': 'hdpmaster1.tm.com.my:8050',
    'columns': None
}

template = '''
<process name="process-INGEST-%(schema)s-%(table)s">
    <tags>type=ingestion source=%(source_name)s schema=%(schema)s ingest_type=%(ingest_type)s</tags>
    <clusters>
        <cluster name="TMDATALAKEP">
            <validity start="2016-01-01T00:00Z" end="2100-01-01T00:00Z"/>
        </cluster>
    </clusters>
    <parallel>1</parallel>
    <order>FIFO</order>
    <frequency>hours(%(frequency_hours)s)</order>
    <timezone>GMT+08:00</timezone>
    <sla shouldStartIn="hours(1)" shouldEndIn="hours(%(should_end_hours)s)"/>
    <validity start="%(start_utc)s" end="2100-01-01T00:00Z" timezone="MYT"/>
    <outputs>
        <output name="output" feed="rawdata-%(schema)s-%(table)s" instance="today(0,0)"/>
    </outputs>
    <workflow engine="oozie" path="/user/trace/workflows/full-ingest/"/>
    <properties>
        %(properties)s
    </properties>
</process>
'''

TYPE_MAP = {
    'VARCHAR2': 'STRING',
    'DATE': 'STRING',
    'NUMBER': 'FLOAT',
    'CHAR': 'STRING',
    'LONG': 'BIGINT',
    'CLOB': 'BINARY',
    'RAW': 'BINARY'
}

for ds in json.loads(open(sys.argv[1]).read()):
    for table in ds['tables']:
        mapper = int((table['estimated_size'] or 0) / 1024 / 1024 / 1024) or 2
        if mapper > 20:
            mapper = 20
        columns = ['`SQOOP_ORACLE_ROWID` STRING']
        for c in table['columns']:
            columns.append('`%s` %s' % (c['field'] , TYPE_MAP[c['type']]))
        properties['jdbc_uri'] = 'jdbc:oracle:thin:@%(host)s:%(port)s/%(tns)s' % {
             'host': ds['datasource']['ip'],
             'port': ds['datasource']['port'],
             'tns': ds['datasource']['tns'],
            
        }
        properties.update({
            'mapper': int((table['estimated_size'] or 0) / 1024 / 1024 / 1024) or 1,
            'source_name': ds['datasource']['name'].replace(' ','_'),
            'username': ds['datasource']['login'],
            'password': ds['datasource']['password'],
            'schema': ds['datasource']['schema'],
            'table': table['table'],
            'split_by': table['split_by'],
            'columns': ', '.join(columns)
        })

        params = {
            'schema': ds['datasource']['schema'],
            'table': table['table'],
            'ingest_type': 'full',
            'source_name': properties['source_name'],
            'start_utc': '2017-01-01T00:00Z',
            'should_end_hours': 5,
            'frequency_hours': 24,
            'properties': '\n       '.join(
                ['<property name="%s" value="%s"/>' % (k,v) for (k,v) in properties.items()])
        }
        job = template % params
        with open('entities/process-%(source_name)s-%(schema)s-%(table)s.xml' % params, 'w') as f:
            f.write(job)
