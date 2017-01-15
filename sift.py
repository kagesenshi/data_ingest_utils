import csv
import json
import sys

data = json.loads(open('profiler-output-2017-01-14-10-20-05.json').read())

result = []

for source in data:
    source_name = source['datasource']['name']
    for tbl in source['tables']:
        size_gb = (tbl['estimated_size'] or 0) / 1024 / 1024 / 1024
        columns = [i['field'] for i in tbl['columns']]
        if size_gb <= 1:
            result.append(('less_than_1', tbl))
        elif size_gb < 5:
            result.append(('less_than_5', tbl))
        else:
            if (tbl['primary_keys'] or tbl['unique_keys']):
                if 'LAST_UPD' in columns:
                    result.append(('more_than_5_has_pkeyukey_has_lastupd', tbl))
                else:
                    result.append(('more_than_5_has_pkeyukey_no_lastupd', tbl))
            else:
                if 'LAST_UPD' in columns:
                    result.append(('more_than_5_no_pkeyukey_has_lastupd', tbl))
                else:
                    result.append(('more_than_5_no_pkeyukey_no_lastupd', tbl))


for i in  result:
    print '%s\t\t\t\t\t%s' % (i[0],i[1]['table'])

