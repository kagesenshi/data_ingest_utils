#!/usr/bin/env python

import subprocess
import argparse
import os, sys
from datetime import datetime
import time
import shutil

def main():
    now = datetime.now()
    utcnow = datetime.utcnow()
    fmt = '%Y-%m-%d %H:%M:%S.0'
    out = {
      'NOW': now.strftime(fmt),
      'UTCNOW': utcnow.strftime(fmt),
      'DATE': utcnow.strftime('%Y-%m-%d')
    }

    print '\n'.join(['%s=%s' % (k, v) for k,v in out.items()])

if __name__ == '__main__':
    main()
