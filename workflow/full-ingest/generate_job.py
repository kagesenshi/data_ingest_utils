import sys
import json

template = '''
resourceManager=hdpmaster1.tm.com.my:8050
queueName=ingestion
nameNode=hdfs://hdpmaster1.tm.com.my:8020
mapper=%(mapper)s
source_name=%(source_name)s
jdbc_uri=jdbc:oracle:thin:@%(host)s:%(port)s/%(tns)s
schema=%(schema)s
table=%(table)s
split_by=%(split_by)s
username=%(username)s
password=%(password)s
oozie.wf.application.path=/user/trace/workflows/%(workflow)s/
oozie.use.system.libpath=true
user.name=trace
mapreduce.job.user.name=trace
jobTracker=hdpmaster1.tm.com.my:8050
columns=%(columns)s
columns_create=%(columns_create)s
columns_java=%(columns_java)s
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

JAVA_TYPE_MAP = {
    'VARCHAR2': 'String',
    'DATE': 'String',
    'NUMBER': 'Float',
    'CHAR': 'String',
    'LONG': 'Long',
    'CLOB': 'Bytes',
    'RAW': 'Bytes'
}


for ds in json.loads(open(sys.argv[1]).read()):
    for table in ds['tables']:
        mapper = int((table['estimated_size'] or 0) / 1024 / 1024 / 1024) or 1
        if mapper > 20:
            mapper = 20


        columns = [c['field'] for c in table['columns']]
        columns_create = []
        columns_java = []
        for c in table['columns']:
            columns_create.append('`%s` %s' % (c['field'] , TYPE_MAP[c['type']]))
            columns_java.append('%s=%s' % (c['field'], TYPE_MAP[c['type']]))
        source_name = ds['datasource']['name'].replace(' ','_')

        username = ds['datasource']['login']
        password = ds['datasource']['password']

#        if source_name in ['SIEBEL_NOVA', 'CPC', 'BRM_NOVA']:
#            workflow = 'full-ingest-direct'
#        else:
#            workflow = 'full-ingest'
        workflow = 'full-ingest-direct'
        params = {
            'mapper': int((table['estimated_size'] or 0) / 1024 / 1024 / 1024) or 2,
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
            'columns': ','.join([c['field'] for c in table['columns']]),
            'workflow': workflow
        }
        job = template % params
        with open('jobs/%(workflow)s-%(source_name)s-%(schema)s-%(table)s.properties' % params, 'w') as f:
            f.write(job)
