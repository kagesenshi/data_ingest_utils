#!/usr/bin/env python
import psutil
import time
import os, socket
import urllib2
import json
from pprint import pprint
import sys
import traceback
import csv
from datetime import datetime

def send_nifi(url, data):
    print data
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    try:
        return urllib2.urlopen(req, data=json.dumps(data))
    except Exception, e:
        traceback.print_exc()

def start(sessionid, sources, nifi_url):
    sqoop_template = (
        "sqoop-import --hive-import --hive-overwrite --create-hive-table "
        "--connect jdbc:oracle:thin:@//%(host)s:%(port)s/%(tns)s --username "
        "%(username)s --password %(password)s --table %(schema)s.%(table)s -m "
        "%(mapper)s --direct "
    )

    data = []
    for row in csv.DictReader(open(sources), delimiter=','):
        data.append(row)

    source_count = len(data) - 1
    for idx, row in enumerate(data):
        start = int(time.time())
        cmd = sqoop_template % row
        if row['split_by'].strip():
            cmd += ' --split-by %s ' % row['split_by']
        cmd += ('2>&1 | tee sqoop-%(schema)s.%(table)s-%(ts)s-err.log') % {
                'schema': row['schema'],
                'table': row['table'], 
                'ts': datetime.fromtimestamp(start).strftime('%Y-%m-%d-%H-%M-%S')}
        exitcode = os.system(cmd)
        end = int(time.time())
        result = {
            'APPLICATION': 'sqoop_tester',
            'START': start,
            'END': end,
            'CMD': cmd,
            'SCHEMA': row['schema'],
            'TABLE': row['table'],
            'SPLIT_BY': row['split_by'],
            'MAPPER': row['mapper'],
            'STATUS': 'fail' if exitcode else 'success'
        }
        send_nifi(nifi_url, result)
        print "%s seconds taken" % (end-start)
        if idx != source_counti:
            print "sleeping for 60seconds"
        time.sleep(60)


    
def main():
    if len(sys.argv) != 4:
        print "Usage: %s [SESSIONID] [SOURCES] [ENDPOINT]" % sys.argv[0]
        print "Run Sqoop test against datasource and send to NiFi Endpoint"
        sys.exit(1)
    start(sys.argv[1], sys.argv[2], sys.argv[3])

if __name__ == '__main__':
    main()

