#!/bin/bash -x

set -e

OUT=`/usr/bin/hive -e "select max($2) from $1"`
echo CHECK_COLUMN_VALUE=$OUT
