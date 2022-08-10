#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE="$M3_PATH"/scripts/docker-integration-tests/prometheus_replication/docker-compose.yml
# quay.io/m3db/prometheus_remote_client_golang @ v0.4.3
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang:v0.4.3
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
export REVISION

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d
#docker-compose -f ${COMPOSE_FILE} up -d dbnode02
#docker-compose -f ${COMPOSE_FILE} up -d coordinator01
#docker-compose -f ${COMPOSE_FILE} up -d coordinator02

function defer {
  :
#  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

echo "Setup dbnode in first cluster"
DBNODE_HOST=dbnode01 \
DBNODE_PORT=9000 \
DBNODE_HEALTH_PORT=9002 \
COORDINATOR_PORT=7201 \
  setup_single_m3db_node

echo "Setup dbnode in second cluster"
DBNODE_HOST=dbnode02 \
DBNODE_PORT=9000 \
DBNODE_HEALTH_PORT=19002 \
COORDINATOR_PORT=17201 \
  setup_single_m3db_node

function prometheus_remote_write {
  local metric_name=$1
  local datapoint_timestamp=$2
  local datapoint_value=$3
  local expect_success=$4
  local expect_success_err=$5
  local expect_status=$6
  local expect_status_err=$7

  network_name="prometheus_replication"
  network=$(docker network ls | fgrep $network_name | tr -s ' ' | cut -f 1 -d ' ' | tail -n 1)
  out=$((docker run -it --rm --network $network           \
    $PROMREMOTECLI_IMAGE                                  \
    -u http://coordinator01:7201/api/v1/prom/remote/write \
    -t __name__:${metric_name}                            \
    -d ${datapoint_timestamp},${datapoint_value} | grep -v promremotecli_log) || true)
  success=$(echo $out | grep -v promremotecli_log | docker run --rm -i $JQ_IMAGE jq .success)
  status=$(echo $out | grep -v promremotecli_log | docker run --rm -i $JQ_IMAGE jq .statusCode)
  if [[ "$success" != "$expect_success" ]]; then
    echo $expect_success_err
    return 1
  fi
  if [[ "$status" != "$expect_status" ]]; then
    echo "${expect_status_err}: actual=${status}"
    return 1
  fi
  echo "Returned success=${success}, status=${status} as expected"
  return 0
}

function test_replication_forwarding {
  now=$(date +"%s")

  # Make sure both are up (otherwise forwarding could fail).
  echo "Test both clusters responding to queries"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s 0.0.0.0:7201/api/v1/query?query=any | jq -r ".data.result | length") -eq 0 ]]'
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s 0.0.0.0:17201/api/v1/query?query=any | jq -r ".data.result | length") -eq 0 ]]'

  # Test writing.
  echo  "Test write data to first cluster"
  prometheus_remote_write \
    "foo_replicate" now 42.42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"

  # Test queries can eventually read back replicated data from second 
  # cluster using port 17201 from the second cluster's coordinator
  echo "Test read replicated data"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s 0.0.0.0:17201/api/v1/query?query=foo_replicate | jq -r ".data.result | length") -gt 0 ]]'
}

# Run all tests
test_replication_forwarding
