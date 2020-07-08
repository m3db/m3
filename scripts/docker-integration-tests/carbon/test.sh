#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/carbon
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
EXPECTED_PATH=$SCRIPT_PATH/expected
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

# Think of this as a defer func() in golang
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
  test "$(echo "$RESPONSE" | jq ".[0].datapoints | .[][0] | select(. != null)" | jq -s last)" = "$expected_val"
  return $?
}

function find_carbon {
  query=$1
  expected_file=$2
  RESPONSE=$(curl -sSg "http://localhost:7201/api/v1/graphite/metrics/find?query=$query")
  ACTUAL=$(echo $RESPONSE | jq '. | sort')
  EXPECTED=$(cat $EXPECTED_PATH/$expected_file | jq '. | sort')
  if [ "$ACTUAL" == "$EXPECTED" ]
  then
    return 0
  fi
  return 1
}

echo "Writing out a carbon metric that should use a min aggregation"
t=$(date +%s)
# 41 should win out here because min(42,41) == 41. Note that technically this test could
# behave incorrectly if the values end up in separate flushes due to the bufferPast
# configuration of the downsampler, but we set reasonable values for bufferPast so that
# should not happen.
echo "foo.min.aggregate.baz 41 $t" | nc 0.0.0.0 7204
echo "foo.min.aggregate.baz 42 $t" | nc 0.0.0.0 7204
echo "Attempting to read min aggregated carbon metric"
ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff read_carbon foo.min.aggregate.baz 41

echo "Writing out a carbon metric that should not be aggregated"
t=$(date +%s)
# 43 should win out here because of M3DB's upsert semantics. While M3DB's upsert
# semantics are not always guaranteed, it is guaranteed for a minimum time window
# that is as large as bufferPast/bufferFuture which should be much more than enough
# time for these two commands to complete.
echo "foo.min.already-aggregated.baz 42 $t" | nc 0.0.0.0 7204
echo "foo.min.already-aggregated.baz 43 $t" | nc 0.0.0.0 7204
echo "Attempting to read unaggregated carbon metric"
ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff read_carbon foo.min.already-aggregated.baz 43

echo "Writing out a carbon metric that should should use the default mean aggregation"
t=$(date +%s)
# Mean of 10 and 20 is 15. Same comment as the min aggregation test above.
echo "foo.min.catch-all.baz 10 $t" | nc 0.0.0.0 7204
echo "foo.min.catch-all.baz 20 $t" | nc 0.0.0.0 7204
echo "Attempting to read mean aggregated carbon metric"
ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff read_carbon foo.min.catch-all.baz 15

# Test writing and reading IDs with colons in them.
t=$(date +%s)
echo "foo.bar:baz.qux 42 $t" | nc 0.0.0.0 7204
ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff read_carbon 'foo.bar:*.*' 42

# Test writing and reading IDs with a single element.
t=$(date +%s)
echo "quail 42 $t" | nc 0.0.0.0 7204
ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff read_carbon 'quail' 42

t=$(date +%s)
echo "a 0 $t"             | nc 0.0.0.0 7204
echo "a.bar 0 $t"         | nc 0.0.0.0 7204
echo "a.biz 0 $t"         | nc 0.0.0.0 7204
echo "a.biz.cake 0 $t"    | nc 0.0.0.0 7204
echo "a.bar.caw.daz 0 $t" | nc 0.0.0.0 7204
echo "a.bag 0 $t"         | nc 0.0.0.0 7204
echo "c:bar.c:baz 0 $t"   | nc 0.0.0.0 7204
ATTEMPTS=10 TIMEOUT=1 retry_with_backoff find_carbon a* a.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon a.b* a.b.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon a.ba[rg] a.ba.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon a.b*.c* a.b.c.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon a.b*.caw.* a.b.c.d.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon x none.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon a.d none.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon *.*.*.*.* none.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon c:* cbar.json
ATTEMPTS=2 TIMEOUT=1 retry_with_backoff find_carbon c:bar.* cbaz.json
