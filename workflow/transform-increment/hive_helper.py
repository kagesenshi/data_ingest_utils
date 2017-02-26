#!/usr/bin/env python

import subprocess
import argparse
import os, sys
from datetime import datetime
import time
import shutil
import tempfile
import re

CREATE_SCRIPT='''
create temporary external table default.%(tablename)s (
    %(table_columns)s
) STORED AS PARQUET
LOCATION "%(path)s";
'''

def guess_type(sql_args):
    query = CREATE_SCRIPT + '''
        select %(check_column)s from default.%(tablename)s where %(check_column)s is
        not null limit 1;
    '''

    result = execute_query(query, **sql_args)
    if re.match('\d+', result):
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

    if re.match(r'\d+-\d+-\d+ \d+:\d+:\d+\.\d+', result):
       result = (
           "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % check_column_value
       )
    return result

def execute_query(query, **kwargs):
    scriptname = tempfile.mktemp()
    script = '%s.sql' % scriptname
    tablename = os.path.basename(scriptname)
    kwargs['tablename'] = 'increment_%s' % (tablename)
    with open(script, 'w') as s:
        s.write(query % kwargs)
    env = os.environ.copy()
    p = subprocess.Popen(['hive','-f', script],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if p.wait() != 0:
        sys.stderr.write(p.stderr.read())
        sys.exit(1)

    result = p.stdout.read().strip()
    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--path', dest='path', required=True)
    parser.add_argument('-d','--table-columns', dest='table_columns', required=True)
    parser.add_argument('-c','--check_column', dest='check_column', required=True)
    parser.add_argument('-o','--operation', dest='operation', default='max')

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
        'table_columns': args.table_columns,
        'check_column': args.check_column,
        'op': args.operation,
    }

    data_type = guess_type(params)
    check_column_value = get_check_column_value(params, data_type=data_type)

    check_column = args.check_column
    if data_type is not None:
        check_column = '%s(%s)' % (data_type, check_column)

    out = {
      'HIVE_CHECK_COLUMN': check_column,
      'CHECK_COLUMN_VALUE': check_column_value,
      'YEAR': now.strftime('%Y'),
      'MONTH': now.strftime('%m'),
      'DAY': now.strftime('%d'),
      'UTCNOW': utcnow.strftime('%Y-%m-%d %H:%M:%S.0'),
      'DATE': now.strftime('%Y-%m-%d')
    }

    print '\n'.join(['%s=%s' % (k, v) for k,v in out.items()])

if __name__ == '__main__':
    main()
