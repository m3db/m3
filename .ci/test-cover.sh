#!/bin/bash
. "$(dirname $0)/variables.sh"

set -e

TARGET=${1:-profile.cov}
LOG=${2:-test.log}

rm $TARGET &>/dev/null || true
echo "mode: count" > $TARGET
echo "" > $LOG

DIRS=""
for DIR in $SRC;
do
  if ls $DIR/*_test.go &> /dev/null; then
    DIRS="$DIRS $DIR"
  fi
done

NPROC=$(getconf _NPROCESSORS_ONLN)
go run .ci/gotestcover/gotestcover.go -race -covermode=atomic -coverprofile=${TARGET} -v -parallelpackages $NPROC $DIRS | tee $LOG

TEST_EXIT=${PIPESTATUS[0]}

find . -not -path '*/vendor/*' | grep \\.tmp$ | xargs -I{} rm {}
echo "test-cover result: $TEST_EXIT"

exit $TEST_EXIT
