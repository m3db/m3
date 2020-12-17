#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/prometheus/test-correctness.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/prometheus/docker-compose.yml
# quay.io/m3db/prometheus_remote_client_golang @ v0.4.3
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
METRIC_NAME_TEST_RESTRICT_WRITE=bar_metric
export REVISION

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

TEST_SUCCESS=false

function defer {
  if [[ "$TEST_SUCCESS" != "true" ]]; then
    echo "Test failure, printing docker-compose logs"
    docker-compose -f ${COMPOSE_FILE} logs
  fi

  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_single_m3db_node

echo "Start Prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d prometheus01

function test_readiness {
  # Check readiness probe eventually succeeds
  echo "Check readiness probe eventually succeeds"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl --write-out "%{http_code}" --silent --output /dev/null 0.0.0.0:7201/ready) -eq "200" ]]'
}

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
  local map_tags_header=${10}

  local optional_tags=""
  for i in $(seq 0 10); do
    local optional_tag_name=$(eval "echo \$TAG_NAME_$i")
    local optional_tag_value=$(eval "echo \$TAG_VALUE_$i")
    if [[ "$optional_tag_name" != "" ]] || [[ "$optional_tag_value" != "" ]]; then
      optional_tags="$optional_tags -t ${optional_tag_name}:${optional_tag_value}"
    fi
  done

  network_name="prometheus"
  network=$(docker network ls | fgrep $network_name | tr -s ' ' | cut -f 1 -d ' ' | tail -n 1)
  out=$((docker run -it --rm --network $network           \
    $PROMREMOTECLI_IMAGE                                  \
    -u http://coordinator01:7201/api/v1/prom/remote/write \
    -t __name__:${metric_name} ${optional_tags}           \
    -h "M3-Metrics-Type: ${metrics_type}"                 \
    -h "M3-Storage-Policy: ${metrics_storage_policy}"     \
    -h "M3-Map-Tags-JSON: ${map_tags_header}"          \
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

function test_prometheus_remote_write_empty_label_name_returns_400_status_code {
  echo "Test write empty name for a label returns HTTP 400"
  now=$(date +"%s")
  TAG_NAME_0="non_empty_name" TAG_VALUE_0="foo" \
    TAG_NAME_1="" TAG_VALUE_1="bar" \
    prometheus_remote_write \
    "foo_metric" $now 42 \
    false "Expected request to fail" \
    400 "Expected request to return status code 400"
}

function test_prometheus_remote_write_empty_label_value_returns_400_status_code {
  echo "Test write empty value for a label returns HTTP 400"
  now=$(date +"%s")
  TAG_NAME_0="foo" TAG_VALUE_0="bar" \
    TAG_NAME_1="non_empty_name" TAG_VALUE_1="" \
    prometheus_remote_write \
    "foo_metric" $now 42 \
    false "Expected request to fail" \
    400 "Expected request to return status code 400"
}

function test_prometheus_remote_write_duplicate_label_returns_400_status_code {
  echo "Test write with duplicate labels returns HTTP 400"
  now=$(date +"%s")
  hour_ago=$(( now - 3600 ))
  TAG_NAME_0="dupe_name" TAG_VALUE_0="foo" \
    TAG_NAME_1="non_dupe_name" TAG_VALUE_1="bar" \
    TAG_NAME_2="dupe_name" TAG_VALUE_2="baz" \
    prometheus_remote_write \
    "foo_metric" $now 42 \
    false "Expected request to fail" \
    400 "Expected request to return status code 400"
}

function test_prometheus_remote_write_too_old_returns_400_status_code {
  echo "Test write into the past returns HTTP 400"
  now=$(date +"%s")
  hour_ago=$(( now - 3600 ))
  prometheus_remote_write \
    "foo_metric" $hour_ago 3.142 \
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

function test_prometheus_remote_write_map_tags {
  echo "Test map tags header works as expected"
  prometheus_remote_write \
    $METRIC_NAME_TEST_RESTRICT_WRITE now 42.42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200" \
    unaggregated "" '{"tagMappers":[{"write":{"tag":"globaltag","value":"somevalue"}}]}'

  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="" return_status_code="" \
    metrics_type="unaggregated" jq_path=".data.result[0].metric.globaltag" expected_value="somevalue" \
    retry_with_backoff prometheus_query_native
}

function test_query_lookback_applied {
  # Note: this test depends on the config in m3coordinator.yml for this test
  # and the following config value "lookbackDuration: 10m".
  echo "Test lookback config respected"
  now=$(date +"%s")
  # Write into past less than the lookback duration.
  eight_mins_ago=$(( now - 480 ))
  prometheus_remote_write \
    "lookback_test" $eight_mins_ago 42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200" \
    "unaggregated"

  # Now query and ensure that the latest timestamp is within the last two steps
  # from now.
  ATTEMPTS=10 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s "0.0.0.0:7201/api/v1/query_range?query=lookback_test&step=15&start=$(expr $(date "+%s") - 600)&end=$(date "+%s")" | jq -r ".data.result[0].values[-1][0]") -gt $(expr $(date "+%s") - 30) ]]'
}

function test_query_limits_applied {
  # Test the default series limit applied when directly querying
  # coordinator (limit set to 100 in m3coordinator.yml)
  # NB: ensure that the limit is not exceeded (it may be below limit).
  echo "Test query limit with coordinator defaults"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s 0.0.0.0:7201/api/v1/query?query=\\{metrics_storage=\"m3db_remote\"\\} | jq -r ".data.result | length") -lt 101 ]]'

  # Test the series limit applied when directly querying
  # coordinator (series limit set by header)
  echo "Test query series limit with coordinator limit header (default errors without RequireExhaustive disabled)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Series: 10" 0.0.0.0:7201/api/v1/query?query=\\{metrics_storage=\"m3db_remote\"\\} | jq ."error" | grep "query exceeded limit") ]]'

  echo "Test query series limit with require-exhaustive headers false"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Series: 2" -H "M3-Limit-Require-Exhaustive: false" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success | jq -r ".data.result | length") -eq 2 ]]'

  echo "Test query series limit with require-exhaustive headers true (below limit therefore no error)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Series: 4" -H "M3-Limit-Require-Exhaustive: true" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success | jq -r ".data.result | length") -eq 3 ]]'

  echo "Test query series limit with require-exhaustive headers true (above limit therefore error)"
  # Test that require exhaustive error is returned
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ -n $(curl -s -H "M3-Limit-Max-Series: 3" -H "M3-Limit-Require-Exhaustive: true" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success | jq ."error" | grep "query exceeded limit") ]]'
  # Test that require exhaustive error is 4xx
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -o /dev/null -w "%{http_code}" -H "M3-Limit-Max-Series: 3" -H "M3-Limit-Require-Exhaustive: true" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success) = "400" ]]'

  # Test the default docs limit applied when directly querying
  # coordinator (docs limit set by header)
  echo "Test query docs limit with coordinator limit header"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Docs: 1" 0.0.0.0:7201/api/v1/query?query=\\{metrics_storage=\"m3db_remote\"\\} | jq -r ".data.result | length") -lt 101 ]]'

  echo "Test query docs limit with require-exhaustive headers false"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Docs: 1" -H "M3-Limit-Require-Exhaustive: false" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success | jq -r ".data.result | length") -eq 3 ]]'

  echo "Test query docs limit with require-exhaustive headers true (below limit therefore no error)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Limit-Max-Docs: 4" -H "M3-Limit-Require-Exhaustive: true" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success | jq -r ".data.result | length") -eq 3 ]]'

  echo "Test query docs limit with require-exhaustive headers true (above limit therefore error)"
  # Test that require exhaustive error is returned
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ -n $(curl -s -H "M3-Limit-Max-Docs: 1" -H "M3-Limit-Require-Exhaustive: true" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success | jq ."error" | grep "query exceeded limit") ]]'
  # Test that require exhaustive error is 4xx
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -o /dev/null -w "%{http_code}" -H "M3-Limit-Max-Docs: 1" -H "M3-Limit-Require-Exhaustive: true" 0.0.0.0:7201/api/v1/query?query=database_write_tagged_success) = "400" ]]'
}

function prometheus_query_native {
  local endpoint=${endpoint:-}
  local query=${query:-}
  local params=${params:-}
  local metrics_type=${metrics_type:-}
  local metrics_storage_policy=${metrics_storage_policy:-}
  local jq_path=${jq_path:-}
  local expected_value=${expected_value:-}
  local return_status_code=${return_status_code:-}

  params_prefixed=""
  if [[ "$params" != "" ]]; then
    params_prefixed='&'"${params}"
  fi

  if [[ "$return_status_code" == "true" ]]; then
    result=$(curl --write-out '%{http_code}' --silent --output /dev/null  \
      -H "M3-Metrics-Type: ${metrics_type}"                               \
      -H "M3-Storage-Policy: ${metrics_storage_policy}"                   \
      "0.0.0.0:7201/m3query/api/v1/${endpoint}?query=${query}${params_prefixed}")
  else
    result=$(curl -s                                    \
      -H "M3-Metrics-Type: ${metrics_type}"             \
      -H "M3-Storage-Policy: ${metrics_storage_policy}" \
      "0.0.0.0:7201/m3query/api/v1/${endpoint}?query=${query}${params_prefixed}" | jq -r "${jq_path}" | head -1)
  fi
  test "$result" = "$expected_value"
  return $?
}

function test_query_restrict_metrics_type {
  now=$(date +"%s")
  hour_ago=$(( $now - 3600 ))
  step="30s"
  params_instant=""
  params_range="start=${hour_ago}"'&'"end=${now}"'&'"step=30s"
  jq_path_instant=".data.result[0].value[1]"
  jq_path_range=".data.result[0].values[][1]"
  return_status_code=""

  # Test restricting to unaggregated metrics
  echo "Test query restrict to unaggregated metrics type (instant)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_instant" return_status_code="$return_status_code" \
    metrics_type="unaggregated" jq_path="$jq_path_instant" expected_value="42.42" \
    retry_with_backoff prometheus_query_native
  echo "Test query restrict to unaggregated metrics type (range)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_range" return_status_code="$return_status_code" \
    metrics_type="unaggregated" jq_path="$jq_path_range" expected_value="42.42" \
    retry_with_backoff prometheus_query_native

  # Test restricting to aggregated metrics
  echo "Test query restrict to aggregated metrics type (instant)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_instant" return_status_code="$return_status_code" \
    metrics_type="aggregated" metrics_storage_policy="15s:10h" jq_path="$jq_path_instant" expected_value="84.84" \
    retry_with_backoff prometheus_query_native
  echo "Test query restrict to aggregated metrics type (range)"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_range" return_status_code="$return_status_code"  \
    metrics_type="aggregated" metrics_storage_policy="15s:10h" jq_path="$jq_path_range" expected_value="84.84" \
    retry_with_backoff prometheus_query_native
}

function test_prometheus_query_native_timeout {
  now=$(date +"%s")
  hour_ago=$(( $now - 3600 ))
  step="30s"
  timeout=".0001s"
  params_instant="timeout=${timeout}"
  params_range="start=${hour_ago}"'&'"end=${now}"'&'"step=30s"'&'"timeout=${timeout}"
  return_status_code="true"
  expected_value="504"

  echo "Test query gateway timeout (instant)"
  endpoint=query query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_instant" \
    metrics_type="unaggregated" return_status_code="$return_status_code" expected_value="$expected_value" \
    prometheus_query_native
  echo "Test query gateway timeout (range)"
  endpoint=query_range query="$METRIC_NAME_TEST_RESTRICT_WRITE" params="$params_range" \
    metrics_type="unaggregated" return_status_code="$return_status_code" expected_value="$expected_value" \
    prometheus_query_native
}

function test_query_restrict_tags {
  # Test the default restrict tags is applied when directly querying
  # coordinator (restrict tags set to hide any restricted_metrics_type="hidden"
  # in m3coordinator.yml)

  # First write some hidden metrics.
  echo "Test write with unaggregated metrics type works as expected"
  TAG_NAME_0="restricted_metrics_type" TAG_VALUE_0="hidden" \
    TAG_NAME_1="foo_tag" TAG_VALUE_1="foo_tag_value" \
    prometheus_remote_write \
    some_hidden_metric now 42.42 \
    true "Expected request to succeed" \
    200 "Expected request to return status code 200"

  # Check that we can see them with zero restrictions applied as an
  # override (we do this check first so that when we test that they
  # don't appear by default we know that the metrics are already visible).
  echo "Test restrict by tags with header override to remove restrict works"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s -H "M3-Restrict-By-Tags-JSON: {}" 0.0.0.0:7201/api/v1/query?query=\\{restricted_metrics_type=\"hidden\"\\} | jq -r ".data.result | length") -eq 1 ]]'

  # Now test that the defaults will hide the metrics altogether.
  echo "Test restrict by tags with coordinator defaults"
  ATTEMPTS=5 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s 0.0.0.0:7201/api/v1/query?query=\\{restricted_metrics_type=\"hidden\"\\} | jq -r ".data.result | length") -eq 0 ]]'
}

function test_series {
  # Test series search with start/end specified
  ATTEMPTS=5 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s "0.0.0.0:7201/api/v1/series?match[]=prometheus_remote_storage_samples_total&start=0&end=9999999999999.99999" | jq -r ".data | length") -eq 1 ]]'

  # Test series search with no start/end specified
  ATTEMPTS=5 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s "0.0.0.0:7201/api/v1/series?match[]=prometheus_remote_storage_samples_total" | jq -r ".data | length") -eq 1 ]]'

  # Test series search with min/max start time using the Prometheus Go
  # min/max formatted timestamps, which is sent as part of a Prometheus
  # remote query.
  # minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
  # maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()
  # minTimeFormatted = minTime.Format(time.RFC3339Nano)
  # maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
  # Which:
  # minTimeFormatted="-292273086-05-16T16:47:06Z"
  # maxTimeFormatted="292277025-08-18T07:12:54.999999999Z"
  ATTEMPTS=5 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    '[[ $(curl -s "0.0.0.0:7201/api/v1/series?match[]=prometheus_remote_storage_samples_total&start=-292273086-05-16T16:47:06Z&end=292277025-08-18T07:12:54.999999999Z" | jq -r ".data | length") -eq 1 ]]'
}

echo "Running readiness test"
test_readiness

echo "Running prometheus tests"
test_prometheus_remote_read
test_prometheus_remote_write_multi_namespaces
test_prometheus_remote_write_empty_label_name_returns_400_status_code
test_prometheus_remote_write_empty_label_value_returns_400_status_code
test_prometheus_remote_write_duplicate_label_returns_400_status_code
test_prometheus_remote_write_too_old_returns_400_status_code
test_prometheus_remote_write_restrict_metrics_type
test_query_lookback_applied
test_query_limits_applied
test_query_restrict_metrics_type
test_prometheus_query_native_timeout
test_query_restrict_tags
test_prometheus_remote_write_map_tags
test_series

echo "Running function correctness tests"
test_correctness

TEST_SUCCESS=true
