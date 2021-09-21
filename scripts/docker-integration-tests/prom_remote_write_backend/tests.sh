#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh

function prometheus_remote_write {
  local metric_name=$1
  local datapoint_timestamp=$2
  local datapoint_value=$3
  local expect_success=$4
  local expect_success_err=$5
  local expect_status=$6
  local expect_status_err=$7

  network_name="prom_remote_write_backend_backend"
  network=$(docker network ls | grep -F $network_name | tr -s ' ' | cut -f 1 -d ' ' | tail -n 1)

  out=$( (docker run -it --rm --network "$network"          \
    "$PROMREMOTECLI_IMAGE"                                  \
    -u http://m3coordinator01:7201/api/v1/prom/remote/write \
    -t __name__:"${metric_name}"                            \
    -d "${datapoint_timestamp}","${datapoint_value}" | grep -v promremotecli_log) || true)

  success=$(echo "$out" | grep -v promremotecli_log | docker run --rm -i "$JQ_IMAGE" jq .success)
  status=$(echo "$out" | grep -v promremotecli_log | docker run --rm -i "$JQ_IMAGE" jq .statusCode)
  if [[ "$success" != "$expect_success" ]]; then
    echo "$expect_success_err"
    return 1
  fi
  if [[ "$status" != "$expect_status" ]]; then
    echo "${expect_status_err}: actual=${status}"
    return 1
  fi
  echo "Returned success=${success}, status=${status} as expected"
  return 0
}

function wait_until_ready {
  host=$1
  # Check readiness probe eventually succeeds
  echo "Check readiness probe eventually succeeds"
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    "[[ \$(curl --write-out \"%{http_code}\" --silent --output /dev/null $host/ready) -eq \"200\" ]]"
}

function query_metric {
  metric_name=$1
  host=$2
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    "[[ \$(curl -sSf $host/api/v1/query?query=$metric_name | jq -r .data.result[0].value[1]) -gt 0 ]]"
}

function test_prometheus_remote_write_multi_namespaces {
  now=$(date +"%s")
  now_truncate_by=$(( now % 5 ))
  now_truncated=$(( now - now_truncate_by ))
  prometheus_raw_local_address="0.0.0.0:9090"
  prometheus_agg_local_address="0.0.0.0:9091"
  metric_name=foo_metric

  for _ in {1..5} ; do
    prometheus_remote_write \
      $metric_name $now_truncated 42 \
      true "Expected request to succeed" \
      200 "Expected request to return status code 200"
  done

  # Make sure we're proxying writes to the aggregated namespace
  echo "Wait until data begins being written to remote storage for the aggregated namespace"
  query_metric $metric_name $prometheus_raw_local_address

  # Make sure we're proxying writes to the unaggregated namespace
  echo "Wait until data begins being written to remote storage for the unaggregated namespace"
  query_metric $metric_name $prometheus_agg_local_address
}