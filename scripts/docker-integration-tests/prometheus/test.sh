#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/prometheus/docker-compose.yml
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang:v0.3.0
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
export REVISION

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

# think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_single_m3db_node

echo "Start Prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d prometheus01

function test_prometheus_remote_read {
  # Ensure Prometheus can proxy a Prometheus query
  echo "Wait until the remote write endpoint generates and allows for data to be queried"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=prometheus_remote_storage_succeeded_samples_total | jq -r .data.result[].value[1]) -gt 100 ]]'
}

function test_prometheus_remote_write_multi_namespaces {
  # Make sure we're proxying writes to the unaggregated namespace
  echo "Wait until data begins being written to remote storage for the unaggregated namespace"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace=\"unagg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'

  # Make sure we're proxying writes to the aggregated namespace
  echo "Wait until data begins being written to remote storage for the aggregated namespace"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace=\"agg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'
}

function test_prometheus_remote_write_too_old_returns_400_status_code {
  # Test writing too far into the past returns an HTTP 400 status code
  echo "Test write into the past returns HTTP 400"
  hour_ago=$(expr $(date +"%s") - 3600) 
  network=$(docker network ls --format '{{.ID}}' | tail -n 1)
  out=$(docker run -it --rm --network $network $PROMREMOTECLI_IMAGE -u http://coordinator01:7201/api/v1/prom/remote/write -t=__name__:foo -d=${hour_ago},3.142)
  success=$(echo $out | grep -v promremotecli_log | docker run --rm -i $JQ_IMAGE jq .success)
  status=$(echo $out | grep -v promremotecli_log | docker run --rm -i $JQ_IMAGE jq .statusCode)
  if [[ "$success" != "false" ]]; then
    echo "Expected request to fail"
    return 1
  fi
  if [[ "$status" != "400" ]]; then
    echo "Expected request to return status code 400: actual=${status}"
    return 1
  fi
  echo "Returned 400 status code as expected"
  return 0
}

function test_query_limits_applied {
  # Test the default series limit applied when directly querying 
  # coordinator (limit set to 100 in m3coordinator.yml)
  echo "Test query limit with coordinator defaults"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[[ $(curl -s 0.0.0.0:7201/api/v1/query?query=\\{name!=\"\"\\} | jq -r ".data.result | length") -eq 100 ]]'

  # Test the default series limit applied when directly querying 
  # coordinator (limit set by header)
  echo "Test query limit with coordinator limit header"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Series: 10" 0.0.0.0:7201/api/v1/query?query=\\{name!=\"\"\\} | jq -r ".data.result | length") -eq 10 ]]'
}

# Run all tests
test_prometheus_remote_read
test_prometheus_remote_write_multi_namespaces
test_prometheus_remote_write_too_old_returns_400_status_code
test_query_limits_applied


