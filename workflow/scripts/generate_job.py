import sys
import json


hive_create_template = '''
drop table if exists staging.%(source_name)s_%(schema)s_%(table)s;
create external table staging.%(source_name)s_%(schema)s_%(table)s (
   %(columns_create_newline)s
)            
STORED AS PARQUET
LOCATION '/user/trace/source/%(source_name)s/%(schema)s_%(table)s/CURRENT';
'''


oozie_template = '''
resourceManager=hdpmaster1.tm.com.my:8050
queueName=ingestion
nameNode=hdfs://hdpmaster1.tm.com.my:8020
mapper=%(mapper)s
source_name=%(source_name)s
jdbc_uri=jdbc:oracle:thin:@%(host)s:%(port)s/%(tns)s
schema=%(schema)s
table=%(table)s
split_by=%(split_by)s
merge_column=%(merge_column)s
check_column=%(check_column)s
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


hive_create = []
for ds in json.loads(open(sys.argv[1]).read()):
    for table in ds['tables']:
        mapper = int((table['estimated_size'] or 0) / 1024 / 1024 / 1024) or 2
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

        workflow = 'full-ingest'
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
            'columns': ','.join([c['field'] for c in table['columns']]),
            'workflow': workflow,
            'merge_column': table['merge_key'],
            'check_column': table['check_column'],
        }

        # generate full ingest scripts
        job = oozie_template % params
        with open('full-ingest-jobs/%(workflow)s-%(source_name)s-%(schema)s-%(table)s.properties' % params, 'w') as f:
            f.write(job)

        # generate incremental ingest scripts
        if params['merge_column'] and params['check_column']:
            params2 = params.copy()
            params2['workflow'] = 'incremental-ingest'
            job = oozie_template % params2
            with open('incremental-ingest-jobs/%(workflow)s-%(source_name)s-%(schema)s-%(table)s.properties' % params2, 'w') as f:
                f.write(job)

        # generate hive create table
        hive_create.append(hive_create_template % params) 

with open('external-tables.sql', 'w') as f:
     f.write('\n\n'.join(hive_create))
