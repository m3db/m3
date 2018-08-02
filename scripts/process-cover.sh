#!/bin/bash

if [ $# -ne 1 ] || [ -z "$1" ]; then
    echo "usage: $0 <coverfile.out>"
    exit 1
fi

COVERFILE=$1
SUBMIT_COVER="$(dirname $0)/../.ci/codecov.sh"

TARGETS=("dbnode" "query" "m3ninx")
target_patterns() {
    case $1 in
        'dbnode') echo "^mode|github.com/m3db/m3db/src/dbnode|github.com/m3db/m3db/src/cmd/services/m3dbnode";;
        'query') echo "^mode|github.com/m3db/m3db/src/query|github.com/m3db/m3db/src/cmd/services/m3coordinator";;
        'm3ninx') echo "^mode|github.com/m3db/m3db/src/m3ninx";;
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