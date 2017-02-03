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
    parser.add_argument('-b','--split-by', dest='split', required=True)
    parser.add_argument('-c','--connection', dest='connection', required=True)

    args = parser.parse_args()
    
    def now():
        return datetime.now().isoformat()
    
    try:
        con = ora.connect(args.connection)
        con_ora = con.cursor()
        ora_result = con_ora.execute("select min(%s) as min_oracle, max(%s) as max_oracle, count(*) as total_oracle from %s.%s" % (args.split,args.split,args.schema,args.table))
        result = ora_result.fetchall()
        for item in result:
            print 'Min Oracle: %s' %  item[0]
            print 'Max Oracle: %s' %  item[1]
            print 'Total Oracle: %s' %  item[2]
        con_ora.close()
        con.close()
    except Exception, e:
        print e

if __name__ == '__main__':
    main()

