#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/carbon/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

# think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_single_m3db_node

function read_carbon {
  target=$1
  expected_val=$2
  end=$(date +%s)
  start=$(($end-1000))
  RESPONSE=$(curl -sSfg "http://localhost:7201/api/v1/graphite/render?target=$target&from=$start&until=$end")
  test "$(echo "$RESPONSE" | jq ".[0].datapoints | .[][0] | select(. != null)" | tail -n 1)" = "$expected_val"
  return $?
}

echo "Writing out a carbon metric that should use a min aggregation"
t=$(date +%s)
# 41 should win out here because min(42,41) == 41.
echo "foo.min.aggregate.baz 41 $t" | nc 0.0.0.0 7204
echo "foo.min.aggregate.baz 42 $t" | nc 0.0.0.0 7204
echo "Attempting to read min aggregated carbon metric"
ATTEMPTS=10 TIMEOUT=1 retry_with_backoff read_carbon foo.min.aggregate.baz 41

echo "Writing out a carbon metric that should not be aggregated"
t=$(date +%s)
# 43 should win out here because of M3DB's upsert semantics. While M3DB's upsert
# semantics are not always guaranteed, it is guaranteed for a minimum time window
# that is as large as bufferPast/bufferFuture which should be much more than enough
# time for these two commands to complete.
echo "foo.min.already-aggregated.baz 42 $t" | nc 0.0.0.0 7204
echo "foo.min.already-aggregated.baz 43 $t" | nc 0.0.0.0 7204
echo "Attempting to read unaggregated carbon metric"
ATTEMPTS=10 TIMEOUT=1 retry_with_backoff read_carbon foo.min.already-aggregated.baz 43

echo "Writing out a carbon metric that should should use the default mean aggregation"
t=$(date +%s)
# Mean of 10 and 20 is 15.
echo "foo.min.catch-all.baz 10 $t" | nc 0.0.0.0 7204
echo "foo.min.catch-all.baz 20 $t" | nc 0.0.0.0 7204
echo "Attempting to read mean aggregated carbon metric"
ATTEMPTS=10 TIMEOUT=1 retry_with_backoff read_carbon foo.min.catch-all.baz 15

