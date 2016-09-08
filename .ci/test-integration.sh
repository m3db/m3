#!/bin/bash
. "$(dirname $0)/variables.sh"

set -e

TESTS=$(go test -test.v -test.tags=integration -test.short ./integration | grep SKIP | cut -d ' ' -f 3)

for TEST in $TESTS; do
  go test -test.v -test.tags=integration -test.run $TEST ./integration
  sleep 0.1
done
