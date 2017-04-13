#!/usr/bin/env python

import argparse
import sys
import os
REGISTRY = {}


def command(name):
    def decorator(func):
        REGISTRY[name] = func
    return decorator


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=REGISTRY.keys())
    args = parser.parse_args()
    if os.path.dirname(__file__):
        os.chdir(os.path.dirname(__file__))
    REGISTRY[args.command]()


@command('builddeps')
def builddeps():
    if not os.path.exists('venv'):
        os.system('virtualenv venv')
#    os.system('./venv/bin/pip install -r requirements.txt')
    os.system('./venv/bin/pip install -e .')
    print "========= Dependencies Build Complete ========="


@command('serve')
def serve():
    os.system('./venv/bin/pserve app.ini')

if __name__ == '__main__':
    main()
