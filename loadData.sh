#!/usr/bin/env bash
set -e

HOST=http://localhost:9999
CONTEXT=bigdata
START=1
END=100000
LOCATION=`pwd`
FORMAT=wikidump-%09d.ttl.gz

while getopts h:c:n:s:e:d:f: option
do
  case "${option}"
  in
    h) HOST=${OPTARG};;
    c) CONTEXT=${OPTARG};;
    n) NAMESPACE=${OPTARG};;
    s) START=${OPTARG};;
    e) END=${OPTARG};;
    d) LOCATION=${OPTARG};;
    f) FORMAT=${OPTARG};;
  esac
done

if [ -z "$NAMESPACE" ]
then
  echo "Usage: $0 -n <namespace> [-h <host>] [-c <context>] [-s <start>] [-e <end>] [-d <directory>] [-f <format>]"
  exit 1
fi

i=$START
while [ $i -le $END ]; do
  printf -v f $FORMAT $i
  if [ ! -f "$LOCATION/$f" ]; then
    echo File $LOCATION/$f not found, terminating
    exit 0
  fi

  echo Processing $f
  curl --silent --show-error --retry 10 --retry-delay 30 --retry-connrefused -XPOST --data-binary update="LOAD <file://$LOCATION/$f>" $HOST/$CONTEXT/namespace/$NAMESPACE/sparql
  let i++
done
