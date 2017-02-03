#!/usr/bin/env python

import cx_Oracle as ora
import pyhs2
import sys
import csv
import traceback
import argparse
from datetime import datetime

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t','--table', dest='table', required=True)
    parser.add_argument('-s','--schema', dest='schema', required=True)
    parser.add_argument('-l','--column', dest='column', required=True)
    parser.add_argument('-c','--connection', dest='connection', required=True)

    args = parser.parse_args()
    
    con = ora.connect(args.connection)
    cur = con.cursor()
    result = cur.execute("""
         select 
              min(%s) as vmin, 
              max(%s) as vmax,
              count(*) as total
         from %s.%s""" % (args.column,args.column,args.schema,args.table))
    for item in result.fetchall():
        print 'MIN=%s' %  item[0]
        print 'MAX=%s' %  item[1]
        print 'TOTAL=%s' %  item[2]
    cur.close()
    con.close()

if __name__ == '__main__':
    main()

