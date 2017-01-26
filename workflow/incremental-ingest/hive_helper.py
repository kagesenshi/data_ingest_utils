#!/usr/bin/env python

import subprocess
import argparse
import os, sys
from datetime import datetime
import time
import shutil
import tempfile

SCRIPT='''
create temporary external table default.%(tablename)s (
    %(table_columns)s
) STORED AS PARQUET
LOCATION "%(path)s";

select %(op)s(%(check_column)s) from default.%(tablename)s;
'''


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

    scriptname = tempfile.mktemp()
    script = '%s.sql' % scriptname
    tablename = os.path.basename(scriptname)
    with open(script, 'w') as s:
        s.write(SCRIPT % {
             'path': args.path,
             'table_columns': args.table_columns,
             'check_column': args.check_column,
             'op': args.operation,
             'tablename': 'increment_%s' % (tablename)
        })
    env = os.environ.copy()
    p = subprocess.Popen(['hive','-f', script],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if p.wait() != 0:
        sys.stderr.write(p.stderr.read())
        sys.exit(1)

    out = {
      'CHECK_COLUMN_VALUE': p.stdout.read().strip(),
      'YEAR': now.strftime('%Y'),
      'MONTH': now.strftime('%m'),
      'DAY': now.strftime('%d'),
      'UTCNOW': utcnow.strftime('%Y-%m-%d %H:%M:%S.0')
    }

    print '\n'.join(['%s=%s' % (k, v) for k,v in out.items()])

if __name__ == '__main__':
    main()
