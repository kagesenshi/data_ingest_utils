#!/usr/bin/env python

import subprocess
import argparse
import os, sys
from datetime import datetime, timedelta
import time
import shutil
import tempfile
import re

CREATE_SCRIPT='''
SET tez.queue.name=%(queue)s;

CREATE TEMPORARY TABLE default.%(tablename)s_SCHEMA
ROW FORMAT SERDE
   'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS AVRO
TBLPROPERTIES (
   'avro.schema.url'='%(nameNode)s/%(path)s/.metadata/schema.avsc'
);

CREATE TEMPORARY EXTERNAL TABLE default.%(tablename)s LIKE
    default.%(tablename)s_SCHEMA
STORED AS PARQUET
LOCATION "%(path)s";
'''

DATE_PATTERN=re.compile(r'^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)\.(\d+)$')

def parse_date(datestr):
    match = DATE_PATTERN.match(datestr)
    if match:
        y,m,d,H,M,S,ms = [int(x) for x in match.groups()]
        return datetime(year=y,month=m,day=d,hour=H,minute=M,second=S)
    raise ValueError("%s is not a date" % datestr)

def guess_type(sql_args):
    if sql_args['check_column'].upper() == 'MOD_T':
        return 'bigint'
    if sql_args['check_column'].upper() == 'LAST_UPD':
        return None

    query = CREATE_SCRIPT + '''
        select %(check_column)s from default.%(tablename)s where %(check_column)s is
        not null limit 1;
    '''

    result = execute_query(query, **sql_args)
    if re.match(r'^\d+$', result):
        return 'bigint'
    return None

def get_check_column_value(sql_args, data_type=None):
    query = CREATE_SCRIPT
    if data_type is not None:
        query += '''
            select %%(op)s(
                    %(data_type)s(
                            %%(check_column)s
                    )
            ) from default.%%(tablename)s;
        ''' % { 'data_type': data_type }
    else:
        query += '''
            select %(op)s(%(check_column)s) from default.%(tablename)s;
        '''
    result = execute_query(query, **sql_args)
    return result

def get_check_column_value_query(value, data_type=None):
    if DATE_PATTERN.match(value):
       value = (
           "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % value
       )
    return value

def backdate_check_column_value(value, backdate, data_type=None):
    if DATE_PATTERN.match(value):
        dt = parse_date(value)
        newdt = dt - timedelta(days=backdate)
        return newdt.strftime('%Y-%m-%d %H:%M:%S.00')
    if re.match(r'^\d+$', value):
        dt = datetime.fromtimestamp(int(value))
        newdt = dt - timedelta(days=backdate)
        return newdt.strftime('%s')
    return value

def execute_query(query, **kwargs):
    queue = kwargs.get('queue', 'default')
    scriptname = tempfile.mktemp()
    script = '%s.sql' % scriptname
    tablename = os.path.basename(scriptname)
    kwargs['tablename'] = 'increment_%s' % (tablename)
    with open(script, 'w') as s:
        s.write(query % kwargs)
    env = os.environ.copy()
    p = subprocess.Popen(['hive','--hiveconf', 'tez.queue.name=%s' % queue, 
                '-f', script],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if p.wait() != 0:
        sys.stderr.write(p.stderr.read())
        sys.exit(1)

    result = p.stdout.read().strip()
    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--path', dest='path', required=True)
    parser.add_argument('-c','--check_column', dest='check_column', required=True)
    parser.add_argument('-o','--operation', dest='operation', default='max')
    parser.add_argument('-n','--namenode', dest='namenode', required=True)
    parser.add_argument('-b','--backdate-days', dest='backdate', default=3,
                                    type=int)
    parser.add_argument('-Q','--queue', dest='queue', default='default')

    hive = '/usr/bin/hive'
    args = parser.parse_args()
    now = datetime.now()
    utcnow = datetime.utcnow()

    if not os.path.exists(hive):
        sys.stderr.write('Hive Client is not installed')
        sys.exit(1)
    if not os.path.exists('/usr/hdp/current/tez-client'):
        sys.stderr.write('Tez Client is not installed')
        sys.exit(1)

    params = {
        'path': args.path,
        'check_column': args.check_column,
        'op': args.operation,
        'backdate': args.backdate,
        'queue': args.queue,
        'nameNode': args.namenode
    }

    data_type = guess_type(params)
    raw_check_column_value = get_check_column_value(params, data_type=data_type)
    backdated_check_column_value = backdate_check_column_value(
        raw_check_column_value, backdate=args.backdate, data_type=data_type,
    )
    check_column_value = get_check_column_value_query(
        backdated_check_column_value, data_type=data_type
    )

    check_column = args.check_column
    hive_check_column_value = backdated_check_column_value
    if data_type is not None:
        check_column = '%s(%s)' % (data_type, check_column)
        hive_check_column_value = '%s("%s")' % (data_type,
                hive_check_column_value)
    elif not re.match('^\d+$', hive_check_column_value):
        hive_check_column_value = '"%s"' % hive_check_column_value

    out = {
      'HIVE_CHECK_COLUMN': check_column,
      'HIVE_CHECK_COLUMN_VALUE': hive_check_column_value,
      'CHECK_COLUMN': args.check_column,
      'CHECK_COLUMN_VALUE': check_column_value,
      'UTCNOW': utcnow.strftime('%Y-%m-%d %H:%M:%S.0'),
      'DATE': now.strftime('%Y-%m-%d')
    }

    print '\n'.join(['%s=%s' % (k, v) for k,v in out.items()])

if __name__ == '__main__':
    main()
