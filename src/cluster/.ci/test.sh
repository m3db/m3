#!/bin/bash

set -o pipefail
set -ex

. $(dirname $0)/build-common.sh

ERROR_LOG="error.log"

readonly COMMIT_MESSAGE=$(git show -s --format=%B HEAD)

comment() {
  echo "$1"
}

set +e

RACE=-race make test-xml test_target="./src/$PACKAGE/" junit_xml=junit.xml 2> $ERROR_LOG

TEST_EXIT=$?

# excludes returns arguments for egrep -v for excludes from .excludelint
excludes() {
    cat .excludelint | sed -e 's/^/ -e /'  | tr -d '\n'
    # grep returns non-zero status when nothing matches, which we want to ignore.
    echo -n " || true"
}

if [ $TEST_EXIT -ne 0 ] ; then
  comment "Testing failed with error code ${TEST_EXIT}"
  comment ""
  awk '/--- FAIL/,/===/' test.log
  comment ""
  comment "Stderr contains:"
  comment ""
  cat $ERROR_LOG
  exit $TEST_EXIT
fi

cat $ERROR_LOG

set -e

echo "Looking for possible data races"
RACES=$(grep -A 10 -e "^WARNING: DATA RACE" $ERROR_LOG || true)
if [ -n "$RACES" ]; then
  comment "Races on: "
  comment ""
  comment "$RACES"
  exit 4
fi

CHECK_PATHS=$(find ./src/$PACKAGE/ -mindepth 1 -maxdepth 1 -type d -not -path '*/.git*' -not -path '*/Godeps' -not -path '*/vendor')

echo "Ensuring code is properly formatted with gofmt"
FMT_ERRORS=$(gofmt -s -e -d ${CHECK_PATHS})
if [ -n "$FMT_ERRORS" ]; then
  comment "Fmt failures on:"
  comment "$FMT_ERRORS"
  echo "Great sadness."
  exit 1
fi

echo "Ensuring code is properly linted with golint"
LINT_EXCLUDE=$(excludes golint)
LINT_ERRORS=$(golint ./src/$PACKAGE/... | eval egrep -q -v $LINT_EXCLUDE)
if [ -n "$LINT_ERRORS" ]; then
  comment "Lint failures on:"
  comment "$LINT_ERRORS"
  echo "Great sadness."
  exit 1
fi

VET_ERRORS=$(go tool vet -methods=false ${CHECK_PATHS} 2>&1)
if [ -n "$VET_ERRORS" ]; then
  comment "Vet failures on:"
  comment "$VET_ERRORS"
  exit 1
fi
