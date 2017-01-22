#!/usr/bin/env python

import subprocess
import argparse
import os, sys
from datetime import datetime
import time
import shutil

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t','--table', dest='table', required=True)
    parser.add_argument('-c','--column', dest='column', required=True)
    parser.add_argument('-o','--operation', dest='operation', default='max')

    hive = '/usr/bin/hive'
    args = parser.parse_args()
    now = datetime.now()
    if not os.path.exists(hive):
        sys.stderr.write('Hive Client is not installed')
        sys.exit(1)
    if not os.path.exists('/usr/hdp/current/tez-client'):
        sys.stderr.write('Tez Client is not installed')
        sys.exit(1)

    env = os.environ.copy()
    p = subprocess.Popen(['hive',
        '-e','select %(op)s(%(column)s) from %(table)s' % {
            'table': args.table,
            'column': args.column,
            'op': args.operation
            }],stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
               env=env)
    if p.wait() != 0:
        sys.stderr.write(p.stderr.read())
        sys.exit(1)
    out = {
      'CHECK_COLUMN_VALUE': p.stdout.read().strip(),
      'YEAR': now.strftime('%Y'),
      'MONTH': now.strftime('%m'),
      'DAY': now.strftime('%d')
    }
    print '\n'.join(['%s=%s' % (k, v) for k,v in out.items()])

if __name__ == '__main__':
    main()
