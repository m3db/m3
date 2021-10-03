#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
source "$M3_PATH"/scripts/docker-integration-tests/prom_remote_write_backend/utils.sh

function test_prometheus_remote_write_multi_namespaces {
  now=$(date +"%s")
  now_truncate_by=$(( now % 5 ))
  now_truncated=$(( now - now_truncate_by ))
  prometheus_raw_local_address="0.0.0.0:9090"
  prometheus_agg_local_address="0.0.0.0:9091"
  metric_name=prom_remote_write_test_metric

  # NB(antanas): just sending metrics multiple times to make sure everything is stable after startup.
  for _ in {1..10} ; do
    prometheus_remote_write \
      $metric_name $now_truncated 42 \
      true "Expected request to succeed" \
      200 "Expected request to return status code 200"
  done

  echo "Querying for data in raw prometheus"
  query_metric $metric_name $prometheus_raw_local_address

  echo "Querying for data in aggregated prometheus"
  query_metric "${metric_name}_rolled_up" $prometheus_agg_local_address
}