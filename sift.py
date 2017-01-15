import csv
import json
import sys

data = json.loads(open(sys.argv[1]).read())

result = []
full_source = []

for ds in csv.DictReader(open(sys.argv[2]), delimiter=','):
    for source in data:
        source_name = source['datasource']['name']
        if ds['source'].upper() == source_name.upper():
            for tbl in source['tables']:
                if str(tbl['table']) == ds['table']:
                    full_source.append((ds,tbl))

for row in full_source:
    ds,tbl = row
    source_name = ds['source']
    category = ds['category'].upper()
    size_gb = (tbl['estimated_size'] or 0) / 1024 / 1024 / 1024
    columns = [i['field'] for i in tbl['columns']]
    has_lastupd = [i for i in columns if 'LAST_UPD' in i]
    has_created = [i for i in columns if 'CREATED' in i]

    if size_gb < 5:
        if category == 'INCREMENTAL':
            result.append(('less_than_5_incremental', row))
        else:
            result.append(('less_than_5_full_ingestion',row))
    else:
        if category == 'INCREMENTAL':
            if (tbl['primary_keys'] or tbl['unique_keys']):

                if has_lastupd:
                    result.append(('more_than_5_incremental_has_pkeyukey_has_lastupd', row))
                else:
                    result.append(('more_than_5_incremental_has_pkeyukey_no_lastupd', row))
            else:
                if has_lastupd and has_created:
                    result.append(('more_than_5_incremental_no_pkeyukey_has_lastupd_and_has_created', row))
                elif has_lastupd or has_created:
                    result.append(('more_than_5_incremental_no_pkeyukey_has_lastupd_or_has_created', row))
                else:
                    result.append(('more_than_5_incremental_no_pkeyukey_no_lastupd_no_created', row))
        else:
            if (tbl['primary_keys'] or tbl['unique_keys']):
                if 'LAST_UPD' in columns:
                    result.append(('more_than_5_full_ingestion_has_pkeyukey_has_lastupd', row))
                else:
                    result.append(('more_than_5_full_ingestion_has_pkeyukey_no_lastupd', row))
            else:
                if 'LAST_UPD' in columns:
                    result.append(('more_than_5_full_ingestion_no_pkeyukey_has_lastupd', row))
                else:
                    result.append(('more_than_5_full_ingestion_no_pkeyukey_no_lastupd', row))


header = ['SOURCE','TABLE','SPRINT','PRIORITY','SIZE (GB)','CATEGORY','LAST_UPD','PRIMARY KEYS','UNIQUE KEYS','CREATED','SPLIT BY']


outputs = {}
for type_, i in result:
    outputs.setdefault(type_, [])
    outputs[type_].append(i)

def flat_out(obj):
    return '"%s"' % '\n'.join(obj)

def get_type(obj,cols):
    data = []
    for i in obj:
        for f,t in cols:
            if i == f:
                data.append(i + ' :: ' + t)
    return data

print sum([len(r) for r in outputs.values()])
for o, rows in outputs.items():
    f = open(o + '.csv','w')
    f.writelines('|'.join(header))
    f.writelines('\n')
    for ds,tbl in rows:
	source_name = ds['source']
	table = ds['table']
        priority = ds['priority']
        sprint = ds['sprint']
        category = ds['category'].upper()
        size_gb = str(float(format((tbl['estimated_size'] or 0) / float(1024) / 1024 / 1024,'.2f')))
        columns = [(i['field'],i['type']) for i in tbl['columns']]
        has_lastupd = flat_out([i[0]+" :: "+i[1] for i in columns if 'LAST_UPD' in i[0] and i[1] == 'DATE'])
        has_created = flat_out([i[0]+" :: "+i[1] for i in columns if 'CREATED' in i[0] and i[1] == 'DATE'])
	pk = flat_out(get_type(tbl['primary_keys'],columns))
	unique_keys = flat_out(get_type(tbl['unique_keys'],columns))
        split_by = ','.join(get_type([tbl['split_by']],columns))
        f.writelines('|'.join([source_name,table,sprint,priority,size_gb,category,has_lastupd,pk,unique_keys,has_created,split_by]))
        f.writelines('\n')

