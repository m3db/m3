#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/aggregator/docker-compose.yml
# quay.io/m3db/prometheus_remote_client_golang @ v0.4.3
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
export REVISION

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

echo "Run m3dbnode"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01

# Stop containers on exit
METRIC_EMIT_PID="-1"
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
  if [ "$METRIC_EMIT_PID" != "-1" ]; then
    echo "Kill metric emit process"
    kill $METRIC_EMIT_PID
  fi
}
trap defer EXIT

echo "Setup DB node"
AGG_RESOLUTION=10s AGG_RETENTION=6h setup_single_m3db_node

echo "Initializing aggregator topology"
curl -vvvsSf -X POST -H "Cluster-Environment-Name: override_test_env" localhost:7201/api/v1/services/m3aggregator/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 2,
    "instances": [
        {
            "id": "m3aggregator01",
            "isolation_group": "availability-zone-a",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3aggregator01:6000",
            "hostname": "m3aggregator01",
            "port": 6000
        },
        {
            "id": "m3aggregator02",
            "isolation_group": "availability-zone-b",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3aggregator02:6000",
            "hostname": "m3aggregator02",
            "port": 6000
        }
    ]
}'

echo "Initializing m3msg inbound topic for m3aggregator ingestion from m3coordinators"
curl -vvvsSf -X POST -H "Topic-Name: aggregator_ingest" -H "Cluster-Environment-Name: override_test_env" localhost:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'

# Do this after placement and topic for m3aggregator is created.
echo "Adding m3aggregator as a consumer to the aggregator ingest topic"
curl -vvvsSf -X POST -H "Topic-Name: aggregator_ingest" -H "Cluster-Environment-Name: override_test_env" localhost:7201/api/v1/topic -d '{
  "consumerService": {
    "serviceId": {
      "name": "m3aggregator",
      "environment": "override_test_env",
      "zone": "embedded"
    },
    "consumptionType": "REPLICATED",
    "messageTtlNanos": "600000000000"
  }
}' # msgs will be discarded after 600000000000ns = 10mins

echo "Initializing m3coordinator topology"
curl -vvvsSf -X POST localhost:7201/api/v1/services/m3coordinator/placement/init -d '{
    "instances": [
        {
            "id": "m3coordinator01",
            "zone": "embedded",
            "endpoint": "m3coordinator01:7507",
            "hostname": "m3coordinator01",
            "port": 7507
        }
    ]
}'
echo "Done initializing m3coordinator topology"

echo "Validating m3coordinator topology"
[ "$(curl -sSf localhost:7201/api/v1/services/m3coordinator/placement | jq .placement.instances.m3coordinator01.id)" == '"m3coordinator01"' ]
echo "Done validating topology"

# Do this after placement for m3coordinator is created.
echo "Initializing m3msg outbound topic for m3coordinator ingestion from m3aggregators"
curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: override_test_env" localhost:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'

echo "Adding m3coordinator as a consumer to the aggregator publish topic"
curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: override_test_env" localhost:7201/api/v1/topic -d '{
  "consumerService": {
    "serviceId": {
      "name": "m3coordinator",
      "environment": "default_env",
      "zone": "embedded"
    },
    "consumptionType": "SHARED",
    "messageTtlNanos": "600000000000"
  }
}' # msgs will be discarded after 600000000000ns = 10mins

echo "Running m3coordinator container"
echo "> port 7202 is coordinator API"
echo "> port 7203 is coordinator metrics"
echo "> port 7204 is coordinator graphite ingest"
echo "> port 7507 is coordinator m3msg ingest from aggregator ingest"
docker-compose -f ${COMPOSE_FILE} up -d m3coordinator01
COORDINATOR_API="localhost:7202"

echo "Running m3aggregator containers"
docker-compose -f ${COMPOSE_FILE} up -d m3aggregator01
docker-compose -f ${COMPOSE_FILE} up -d m3aggregator02

echo "Verifying aggregation with remote aggregators"

function read_carbon {
  target=$1
  expected_val=$2
  end=$(date +%s)
  start=$(($end-1000))
  RESPONSE=$(curl -sSfg "http://${COORDINATOR_API}/api/v1/graphite/render?target=$target&from=$start&until=$end")
  test "$(echo "$RESPONSE" | jq ".[0].datapoints | .[][0] | select(. != null)" | jq -s last)" = "$expected_val"
  return $?
}

# Send metric values 40 and 44 every second
echo "Sending unaggregated carbon metrics to m3coordinator"
bash -c 'while true; do t=$(date +%s); echo "foo.bar.baz 40 $t" | nc 0.0.0.0 7204; echo "foo.bar.baz 44 $t" | nc 0.0.0.0 7204; sleep 1; done' &

# Track PID to kill on exit
METRIC_EMIT_PID="$!"

function test_aggregated_graphite_metric {
  # Read back the averaged averaged metric, we configured graphite
  # aggregation policy to average each tile and we are emitting
  # values 40 and 44 to get an average of 42 each tile
  echo "Read back aggregated averaged metric"
  ATTEMPTS=100 TIMEOUT=1 MAX_TIMEOUT=4 retry_with_backoff read_carbon foo.bar.* 42

  echo "Finished with carbon metrics"
  kill $METRIC_EMIT_PID
  export METRIC_EMIT_PID="-1"
}

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
  local metric_type=${metric_type:counter}

  network_name="aggregator"
  network=$(docker network ls | fgrep $network_name | tr -s ' ' | cut -f 1 -d ' ' | tail -n 1)
  out=$((docker run -it --rm --network $network           \
    $PROMREMOTECLI_IMAGE                                  \
    -u http://m3coordinator01:7202/api/v1/prom/remote/write \
    -h M3-Prom-Type:${metric_type}                        \
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
    "0.0.0.0:7202/api/v1/${endpoint}?query=${query}${params_prefixed}" | jq -r "${jq_path}" | jq -s last)
  test "$result" = "$expected_value"
  return $?
}

function dbnode_fetch {
  local namespace=${namespace}
  local id=${id}
  local rangeStart=${rangeStart}
  local rangeEnd=${rangeEnd}
  local jq_path=${jq_path:-}
  local expected_value=${expected_value:-}

  result=$(curl -s                                    \
    "0.0.0.0:9002/fetch" \
    "-d" \
    "{\"namespace\": \"${namespace}\", \"id\": \"${id}\", \"rangeStart\": ${rangeStart}, \"rangeEnd\": ${rangeEnd}}" | jq -r "${jq_path}")
  test "$result" = "$expected_value"
  return $?
}

function test_aggregated_rollup_rule {
  resolution_seconds="10"
  now=$(date +"%s")
  now_truncate_by=$(( $now % $resolution_seconds ))
  now_truncated=$(( $now - $now_truncate_by ))

  echo "Test write with rollup rule"

  # Emit values for endpoint /foo/bar (to ensure right values aggregated)
  write_at="$now_truncated"
  value="42"
  value_rate="22"
  value_inc_by=$(( $value_rate * $resolution_seconds ))
  for i in $(seq 1 10); do
    label0_name="app" label0_value="nginx_edge" \
      label1_name="status_code" label1_value="500" \
      label2_name="endpoint" label2_value="/foo/bar" \
      metric_type="counter" \
      prometheus_remote_write \
      http_requests $write_at $value \
      true "Expected request to succeed" \
      200 "Expected request to return status code 200"
    write_at=$(( $write_at + $resolution_seconds ))
    value=$(( $value + $value_inc_by ))
  done

  # Emit values for endpoint /foo/baz (to ensure right values aggregated)
  write_at="$now_truncated"
  value="84"
  value_rate="4"
  value_inc_by=$(( $value_rate * $resolution_seconds ))
  for i in $(seq 1 10); do
    label0_name="app" label0_value="nginx_edge" \
      label1_name="status_code" label1_value="500" \
      label2_name="endpoint" label2_value="/foo/baz" \
      metric_type="gauge" \
      prometheus_remote_write \
      http_requests $write_at $value \
      true "Expected request to succeed" \
      200 "Expected request to return status code 200"
    write_at=$(( $write_at + $resolution_seconds ))
    value=$(( $value + $value_inc_by ))
  done

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
    metrics_type="aggregated" metrics_storage_policy="10s:6h" \
    retry_with_backoff prometheus_query_native

  # Test by values are rolled up by second, then sum (for endpoint="/foo/bar")
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    endpoint=query_range query="http_requests_by_status_code\{endpoint=\"/foo/baz\"\}" \
    params="$params_range" \
    jq_path="$jq_path" expected_value="4" \
    metrics_type="aggregated" metrics_storage_policy="10s:6h" \
    retry_with_backoff prometheus_query_native
}

function test_metric_type_survives_aggregation {
  now=$(date +"%s")

  echo "Test metric type should be kept after aggregation"

  # Emit values for endpoint /foo/bar (to ensure right values aggregated)
  write_at="$now_truncated"
  value="42"

  metric_type="counter" \
  prometheus_remote_write \
  metric_type_test $now $value \
  true "Expected request to succeed" \
  200 "Expected request to return status code 200"

  start=$(( $now - 3600 ))
  end=$(( $now + 3600 ))
  jq_path=".datapoints[0].annotation"

  echo "Test query metric type"

  # Test by metric types are stored in aggregated namespace
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 \
    namespace="agg" \
    id='{__name__=\"metric_type_test\",label0=\"label0\",label1=\"label1\",label2=\"label2\"}' \
    rangeStart=${start} \
    rangeEnd=${end} \
    jq_path="$jq_path" expected_value="CAEQAQ==" \
    retry_with_backoff dbnode_fetch
}

echo "Run tests"
test_aggregated_graphite_metric
test_aggregated_rollup_rule
test_metric_type_survives_aggregation
