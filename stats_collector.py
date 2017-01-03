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
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    return urllib2.urlopen(req, data=json.dumps(data))


def start(nifi_url):
    hostname = socket.gethostname()
    while True:
        cpu = psutil.cpu_percent(interval=1, percpu=True)
        res = {
           'TIMESTAMP': int(time.time() * 1000),
           'HOSTNAME': hostname,
           'RAM': psutil.virtual_memory().percent
        }
        for idx, c in enumerate(cpu):
            res['CPU%s' % idx] = c
        try:
            send_nifi(nifi_url, res)
        except:
            pass

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: %s [ENDPOINT]" % sys.argv[0]
        print "Send system stats (CPU/RAM) to NiFi ListenHTTP Endpoint"
        sys.exit(1)
    start(sys.argv[1])
