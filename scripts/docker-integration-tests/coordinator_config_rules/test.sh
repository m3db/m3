#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH="$M3_PATH"/scripts/docker-integration-tests/coordinator_config_rules
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
# quay.io/m3db/prometheus_remote_client_golang @ v0.4.3
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
export REVISION

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

# Think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_single_m3db_node_long_namespaces

function prometheus_remote_write {
  local metric_name=$1
  local datapoint_timestamp=$2
  local datapoint_value=$3
  local expect_success=$4
  local expect_success_err=$5
  local expect_status=$6
  local expect_status_err=$7
  local label0_name=${label0_name:-label0}
  local label0_value=${label0_value:-label0}
  local label1_name=${label1_name:-label1}
  local label1_value=${label1_value:-label1}
  local label2_name=${label2_name:-label2}
  local label2_value=${label2_value:-label2}

  network_name="coordinator_config_rules"
  network=$(docker network ls | fgrep $network_name | tr -s ' ' | cut -f 1 -d ' ' | tail -n 1)
  out=$((docker run -it --rm --network $network           \
    $PROMREMOTECLI_IMAGE                                  \
    -u http://coordinator01:7201/api/v1/prom/remote/write \
    -t __name__:${metric_name}                            \
    -t ${label0_name}:${label0_value}                     \
    -t ${label1_name}:${label1_value}                     \
    -t ${label2_name}:${label2_value}                     \
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
    "0.0.0.0:7201/api/v1/${endpoint}?query=${query}${params_prefixed}" | jq -r "${jq_path}" | jq -s last)
  test "$result" = "$expected_value"
  return $?
}

function test_query_mapping_rule {
  now=$(date +"%s")
  now_truncate_by=$(( $now % 5 ))
  now_truncated=$(( $now - $now_truncate_by ))
  now_truncated_plus_second=$(( $now_truncated + 1 ))

  echo "Test write with mapping rule"
  # nginx metrics
  label0_name="app" label0_value="nginx_edge" \
    prometheus_remote_write \
    foo_metric $now_truncated 42.42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"
  label0_name="app" label0_value="nginx_edge" \
    prometheus_remote_write \
    foo_metric $now_truncated_plus_second 84.84 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"

  # mysql metrics
  label0_name="app" label0_value="mysql_db" \
    prometheus_remote_write \
    foo_metric $now_truncated 45.42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"
  label0_name="app" label0_value="mysql_db" \
    prometheus_remote_write \
    foo_metric $now_truncated_plus_second 87.84 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"

  start=$(( $now - 3600 ))
  end=$(( $now + 3600 ))
  step="30s"
  params_range="start=${start}"'&'"end=${end}"'&'"step=30s"
  jq_path=".data.result[0].values[0] | .[1] | select(. != null)"

  # Test values can be mapped to 30s:24h resolution namespace (for app="nginx")
  echo "Test query mapping rule"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query=foo_metric params="$params_range" \
    jq_path="$jq_path" expected_value="84.84" \
    metrics_type="aggregated" metrics_storage_policy="30s:24h" \
    retry_with_backoff prometheus_query_native

  # Test values can be mapped to 1m:48h resolution namespace (for app="mysql")
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query=foo_metric params="$params_range" \
    jq_path="$jq_path" expected_value="87.84" \
    metrics_type="aggregated" metrics_storage_policy="1m:48h" \
    retry_with_backoff prometheus_query_native
}

function test_query_rollup_rule {
  if [ "$TEST_ROLLUP_RULE" != "true" ]; then
    echo "Skip testing rollup rule, timestamped metrics don't work with rollup rules just yet"
    return 0
  fi

  now=$(date +"%s")
  now_truncate_by=$(( $now % 5 ))
  now_truncated=$(( $now - $now_truncate_by ))
  now_truncated_plus_second=$(( $now_truncated + 1 ))

  echo "Test write with rollup rule"

  # Emit values for endpoint /foo/bar (to ensure right values aggregated)
  label0_name="app" label0_value="nginx_edge" \
    label1_name="status_code" label1_value="500" \
    label2_name="endpoint" label2_value="/foo/bar" \
    prometheus_remote_write \
    http_requests $now_truncated 42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"
  label0_name="app" label0_value="nginx_edge" \
    label1_name="status_code" label1_value="500" \
    label2_name="endpoint" label2_value="/foo/bar" \
    prometheus_remote_write \
    http_requests $now_truncated_plus_second 64 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"

  # Emit values for endpoint /foo/baz (to ensure right values aggregated)
  label0_name="app" label0_value="nginx_edge" \
    label1_name="status_code" label1_value="500" \
    label2_name="endpoint" label2_value="/foo/baz" \
    prometheus_remote_write \
    http_requests $now_truncated 8 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"
  label0_name="app" label0_value="nginx_edge" \
    label1_name="status_code" label1_value="500" \
    label2_name="endpoint" label2_value="/foo/baz" \
    prometheus_remote_write \
    http_requests $now_truncated_plus_second 12 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"

  start=$(( $now - 3600 ))
  end=$(( $now + 3600 ))
  step="30s"
  params_range="start=${start}"'&'"end=${end}"'&'"step=30s"
  jq_path=".data.result[0].values | .[][1] | select(. != null)"

  echo "Test query rollup rule"

  # Test by values are rolled up by second, then sum (for endpoint="/foo/bar")
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="http_requests_by_status_code\{endpoint=\"/foo/bar\"\}" \
    params="$params_range" \
    jq_path="$jq_path" expected_value="22" \
    metrics_type="aggregated" metrics_storage_policy="5s:10h" \
    retry_with_backoff prometheus_query_native

  # Test by values are rolled up by second, then sum (for endpoint="/foo/bar")
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="http_requests_by_status_code\{endpoint=\"/foo/baz\"\}" \
    params="$params_range" \
    jq_path="$jq_path" expected_value="4" \
    metrics_type="aggregated" metrics_storage_policy="5s:10h" \
    retry_with_backoff prometheus_query_native
}

echo "Running prometheus mapping and rollup rule tests"
test_query_mapping_rule
test_query_rollup_rule
