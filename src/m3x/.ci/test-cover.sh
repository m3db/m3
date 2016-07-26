#!/bin/bash

set -e

SOURCE=${1:-.}
TARGET=${2:-profile.cov}
LOG=${3:-test.log}

rm $TARGET &>/dev/null || true
echo "mode: count" > $TARGET
echo "" > $LOG

DIRS=""
for DIR in $(find $SOURCE -maxdepth 10 -not -path '*/.git*' -not -path '*/.ci*' -not -path '*/_*' -not -path '*/vendor/*' -type d);
do
  if ls $DIR/*_test.go &> /dev/null; then
    DIRS="$DIRS $DIR"
  fi
done

NPROC=$(getconf _NPROCESSORS_ONLN)
go run .ci/gotestcover/gotestcover.go -covermode=set -coverprofile=${TARGET} -v -parallelpackages $NPROC $DIRS | tee $LOG

TEST_EXIT=${PIPESTATUS[0]}

find . | grep \\.tmp | xargs -I{} rm {}
echo "test-cover result: $TEST_EXIT"

exit $TEST_EXIT
