#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/coordinator_config_rules
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
EXPECTED_PATH=$SCRIPT_PATH/expected
METRIC_NAME_TEST_OLD=foo
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

  network=$(docker network ls --format '{{.ID}}' | tail -n 1)
  out=$((docker run -it --rm --network $network           \
    $PROMREMOTECLI_IMAGE                                  \
    -u http://coordinator01:7201/api/v1/prom/remote/write \
    -t __name__:${metric_name}                            \
    -t foo:bar
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

function prometheus_write_metric {
  echo "Test write with aggregated metrics type works as expected"
  prometheus_remote_write \
    old_metric now 84.84 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200" \
    aggregated 15s:10h
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


function test_query_rollup_rule {
  now=$(date +"%s")
  hour_ago=$(expr $now - 3600) 
  step="30s"
  params_instant=""
  params_range="start=${hour_ago}"'&'"end=${now}"'&'"step=30s"
  jq_path_instant=".data.result[0].value[1]"
  jq_path_range=".data.result[0].metric[\"__name__\"]"

  echo "Test query rollup rule"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query=new_metric params="$params_range" \
    metrics_type="aggregated" metrics_storage_policy="15s:10h" jq_path="$jq_path_range" expected_value="new_metric" \
    retry_with_backoff prometheus_query_native
}

echo "Running prometehus tests"
test_query_rollup_rule
