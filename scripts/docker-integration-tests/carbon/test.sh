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

echo "Writing out a carbon metric"
echo "foo.bar.baz 42 `date +%s`" | nc 0.0.0.0 7204

echo "Attempting to read carbon metric back"
function read_carbon {
  end=$(date +%s)
  start=$(($end-1000))
  RESPONSE=$(curl -sSfg "http://localhost:7201/api/v1/graphite/render?target=foo.bar.*&from=$start&until=$end")
  test "$(echo "$RESPONSE" | jq ".[0].datapoints | .[][0] | select(. != null)" | tail -n 1)" = "42"
  return $?
}
ATTEMPTS=10 TIMEOUT=1 retry_with_backoff read_carbon
