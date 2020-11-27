#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/prometheus/docker-compose.yml
# quay.io/m3db/prometheus_remote_client_golang @ v0.4.3
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
METRIC_NAME_TEST_TOO_OLD=foo
METRIC_NAME_TEST_RESTRICT_WRITE=bar
export REVISION

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

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
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=prometheus_remote_storage_samples_total | jq -r .data.result[0].value[1]) -gt 100 ]]'
}

function test_prometheus_remote_write_multi_namespaces {
  # Make sure we're proxying writes to the unaggregated namespace
  echo "Wait until data begins being written to remote storage for the unaggregated namespace"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace=\"unagg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'

  # Make sure we're proxying writes to the aggregated namespace
  echo "Wait until data begins being written to remote storage for the aggregated namespace"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace=\"agg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'
}

function prometheus_remote_write {
  local metric_name=$1
  local datapoint_timestamp=$2
  local datapoint_value=$3
  local expect_success=$4
  local expect_success_err=$5
  local expect_status=$6
  local expect_status_err=$7
  local metrics_type=$8
  local metrics_storage_policy=$9

  network_name="simple_v2_batch_apis"
  network=$(docker network ls | fgrep $network_name | tr -s ' ' | cut -f 1 -d ' ' | tail -n 1)
  out=$((docker run -it --rm --network $network           \
    $PROMREMOTECLI_IMAGE                                  \
    -u http://dbnode01:7201/api/v1/prom/remote/write \
    -t __name__:${metric_name}                            \
    -h "M3-Metrics-Type: ${metrics_type}"                 \
    -h "M3-Storage-Policy: ${metrics_storage_policy}"     \
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

function test_prometheus_remote_write_too_old_returns_400_status_code {
  # Test writing too far into the past returns an HTTP 400 status code
  echo "Test write into the past returns HTTP 400"
  now=$(date +"%s")
  hour_ago=$(( now - 3600 ))
  prometheus_remote_write \
    $METRIC_NAME_TEST_TOO_OLD $hour_ago 3.142 \
    false "Expected request to fail" \
    400 "Expected request to return status code 400"
}

function test_prometheus_remote_write_restrict_metrics_type {
  # Test we can specify metrics type
  echo "Test write with unaggregated metrics type works as expected"
  prometheus_remote_write \
    $METRIC_NAME_TEST_RESTRICT_WRITE now 42.42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200" \
    unaggregated
  
  echo "Test write with aggregated metrics type works as expected"
  prometheus_remote_write \
    $METRIC_NAME_TEST_RESTRICT_WRITE now 84.84 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200" \
    aggregated 15s:10h
}

function test_query_limits_applied {
  # Test the default series limit applied when directly querying 
  # coordinator (limit set to 100 in m3coordinator.yml)
  echo "Test query limit with coordinator defaults"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s 0.0.0.0:7201/api/v1/query?query=\\{name!=\"\"\\} | jq -r ".data.result | length") -eq 100 ]]'

  # Test the default series limit applied when directly querying 
  # coordinator (limit set by header)
  echo "Test query limit with coordinator limit header"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Series: 10" -H "M3-Limit-Require-Exhaustive: false" 0.0.0.0:7201/api/v1/query?query=\\{name!=\"\"\\} | jq -r ".data.result | length") -eq 10 ]]'
}

function prometheus_query_native {
  local endpoint=${endpoint:-}
  local query=${query:-}
  local params=${params:-}
  local metrics_type=${metrics_type:-}
  local metrics_storage_policy=${metrics_storage_policy:-}
  local jq_path=${jq_path:-}
  local expected_value=${expected_value:-}

  params_prefixed=""
  if [[ "$params" != "" ]]; then
    params_prefixed='&'"${params}"
  fi

  result=$(curl -s                                    \
    -H "M3-Metrics-Type: ${metrics_type}"             \
    -H "M3-Storage-Policy: ${metrics_storage_policy}" \
    "0.0.0.0:7201/api/v1/${endpoint}?query=${query}${params_prefixed}" | jq -r "${jq_path}")
  test "$result" = "$expected_value"
  return $?
}

function test_query_restrict_metrics_type {
  now=$(date +"%s")
  hour_ago=$(( now - 3600 ))

  step="30s"
  params_instant=""
  params_range="start=${hour_ago}"'&'"end=${now}"'&'"step=30s"
  jq_path_instant=".data.result[0].value[1]"
  jq_path_range=".data.result[0].values[][1]"
  
  # Test restricting to unaggregated metrics
  echo "Test query restrict to unaggregated metrics type (instant)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_instant" \
    metrics_type="unaggregated" jq_path="$jq_path_instant" expected_value="42.42" \
    retry_with_backoff prometheus_query_native
  echo "Test query restrict to unaggregated metrics type (range)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_range" \
    metrics_type="unaggregated" jq_path="$jq_path_range" expected_value="42.42" \
    retry_with_backoff prometheus_query_native

  # Test restricting to aggregated metrics
  echo "Test query restrict to aggregated metrics type (instant)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_instant" \
    metrics_type="aggregated" metrics_storage_policy="15s:10h" jq_path="$jq_path_instant" expected_value="84.84" \
    retry_with_backoff prometheus_query_native
  echo "Test query restrict to aggregated metrics type (range)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_range" \
    metrics_type="aggregated" metrics_storage_policy="15s:10h" jq_path="$jq_path_range" expected_value="84.84" \
    retry_with_backoff prometheus_query_native
}

# Run all tests
test_prometheus_remote_read
test_prometheus_remote_write_multi_namespaces
test_prometheus_remote_write_too_old_returns_400_status_code
test_prometheus_remote_write_restrict_metrics_type
test_query_limits_applied
test_query_restrict_metrics_type
