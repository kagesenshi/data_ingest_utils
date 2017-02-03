#!/bin/bash

SCRIPTPATH=`realpath $0`
SCRIPTDIR=`dirname $SCRIPTPATH`
pushd $SCRIPTDIR
cxfreeze source-verification.py --target-dir dist
cp /usr/lib/oracle/12.1/client64/lib/* dist
tar -czvf source-verification.tar.gz dist
rm -rf dist
popd
