import sys
import json

all_ = json.loads(open(sys.argv[1]).read())

TYPE_MAP = {
    'VARCHAR2': 'STRING',
    'DATE': 'STRING',
    'NUMBER': 'STRING',
    'CHAR': 'STRING',
    'LONG': 'BIGINT',
    'CLOB': 'BINARY',
    'RAW': 'BINARY'
}

for ds in all_:
    if ds['datasource']['name'] in ['BRM NOVA', 'SIEBEL NOVA', 'CPC']:
        for table in ds['tables']:
            hive_create_table = """
            drop table if exists staging.%(source_name)s_%(schema)s_%(table)s;
            create external table staging.%(source_name)s_%(schema)s_%(table)s (
               %(columns)s
            )            
            STORED AS PARQUET
            LOCATION '/user/trace/source/%(source_name)s/%(schema)s_%(table)s/CURRENT';
            """
            columns = []
            source_name = ds['datasource']['name'].replace(' ', '_')
            if source_name in ['SIEBEL_NOVA']:
                workflow = 'full-ingest-direct'
            else:
                workflow = 'full-ingest'
            
            for c in table['columns']:
                columns.append('`%s` %s' % (c['field'] , TYPE_MAP[c['type']]))
            print hive_create_table % {
                    'table': table['table'],
                    'columns': ',\n                '.join(columns),
                    'source_name': ds['datasource']['name'].replace(' ','_'),
                    'schema': ds['datasource']['schema']
            }
