#!/usr/bin/env python
# Send system stats (CPU/RAM) to NiFI ListenHTTP Endpoint

import psutil
import time
import os, socket
import urllib2
import json
from pprint import pprint
import sys

def send_nifi(url, data):
    print data
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    return urllib2.urlopen(req, data=json.dumps(data))


def start(sessionid, nifi_url):
    appid = 'stats_collector'
    hostname = socket.gethostname()
    while True:
        net_io_start = psutil.net_io_counters()
        cpu = psutil.cpu_percent(interval=1, percpu=True)
        net_io_end = psutil.net_io_counters()
        res = {
           'APPLICATION': appid, 
           'SESSIONID': sessionid,
           'TIMESTAMP': int(time.time()),
           'HOSTNAME': hostname,
           'RAM': psutil.virtual_memory().percent,
           'CPU': sum(cpu)/len(cpu),
           'NET_BYTES_SENT': net_io_end.bytes_sent - net_io_start.bytes_sent,
           'NET_BYTES_RECV': net_io_end.bytes_recv - net_io_start.bytes_recv,
        }
        for idx, c in enumerate(cpu):
            res['CPU%s' % idx] = c
        try:
            send_nifi(nifi_url, res)
        except:
            pass

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: %s [SESSIONID] [ENDPOINT]" % sys.argv[0]
        print "Send system stats (CPU/RAM) to NiFi ListenHTTP Endpoint"
        sys.exit(1)
    start(sys.argv[1], sys.argv[2])
