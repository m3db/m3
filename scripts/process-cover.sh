#!/bin/bash

if [ $# -ne 1 ] || [ -z "$1" ]; then
    echo "usage: $0 <coverfile.out>"
    exit 1
fi

COVERFILE=$1
SUBMIT_COVER="$(dirname $0)/../.ci/codecov.sh"

TARGETS=("aggregator" "dbnode" "query" "cluster" "m3ninx" "m3em" "x")
target_patterns() {
    case $1 in
        'cluster') echo "^mode|github.com/m3db/m3/src/cluster";;
        'aggregator') echo "^mode|github.com/m3db/m3/src/aggregator|github.com/m3db/m3/src/cmd/services/m3aggregator";;
        'dbnode') echo "^mode|github.com/m3db/m3/src/dbnode|github.com/m3db/m3/src/cmd/services/m3dbnode";;
        'query') echo "^mode|github.com/m3db/m3/src/query|github.com/m3db/m3/src/cmd/services/m3query";;
        'm3em') echo "^mode|github.com/m3db/m3/src/m3em|github.com/m3db/m3/src/cmd/services/m3em_agent";;
        'm3ninx') echo "^mode|github.com/m3db/m3/src/m3ninx";;
        'x') echo "^mode|github.com/m3db/m3/src/x";;
        'msg') echo "^mode|github.com/m3db/m3/src/msg";;
        *)   echo "unknown key: $1"; exit 1;;
    esac
}

if [ ! -f $COVERFILE ]; then
  echo "$COVERFILE does not exist"
  exit 1
fi

for t in ${TARGETS[@]}; do
  cat $COVERFILE | grep -E $(target_patterns $t) > ${t}.out
  ${SUBMIT_COVER} -f ${t}.out -F ${t}
done
