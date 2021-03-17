#!/usr/bin/env bash

set -ex
TEST_PATH="$M3_PATH"/scripts/docker-integration-tests
FANOUT_PATH=$TEST_PATH/query_fanout
source $TEST_PATH/common.sh
source $FANOUT_PATH/warning.sh

function test_restrictions {
  t=$(date +%s)
  METRIC_NAME="foo_$t"
  # # write 5 metrics to cluster a
  write_metrics coordinator-cluster-a 5
  # write 10 metrics to cluster b
  write_metrics coordinator-cluster-b 10

  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff go run $FANOUT_PATH/restrict.go -t $t
}
