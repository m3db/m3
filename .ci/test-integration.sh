#!/bin/bash
. "$(dirname $0)/variables.sh"

set -e

TAGS="integration"
DIR="integration"

# compile the integration test binary
go test -test.c -test.tags=${TAGS} ./${DIR}

# list the tests
TESTS=$(./integration.test -test.v -test.short | grep RUN | tr -s " " | cut -d ' ' -f 3)

# execute tests one by one for isolation
for TEST in $TESTS; do
  ./integration.test -test.v -test.run $TEST ./integration
  TEST_EXIT=$?
  if [ "$TEST_EXIT" != "0" ]; then
    echo "$TEST failed"
    exit $TEST_EXIT
  fi
  sleep 0.1
done

echo "PASS all integrations tests"
