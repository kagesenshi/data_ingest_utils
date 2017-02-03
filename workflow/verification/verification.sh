#!/bin/bash

while [[ $# -gt 1 ]]
do
key="$1"

case $key in
    -t|--table)
    TABLE="$2"
    shift # past argument
    ;;
    -s|--schema)
    SCHEMA="$2"
    shift # past argument
    ;;
    -b|--split-by)
    SPLIT="$2"
    shift # past argument
    ;;
    -c|--connection)
    CONNECTION="$2"
    shift # past argument
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done

tar -xvf source-verification.tar.gz
cd dist
echo "`pwd`"
echo "source-verification -t ${TABLE} -s ${SCHEMA} -b ${SPLIT} -c ${CONNECTION}"
./source-verification -t ${TABLE} -s ${SCHEMA} -b ${SPLIT} -c ${CONNECTION}
