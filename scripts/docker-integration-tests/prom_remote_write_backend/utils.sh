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

function wait_until_leader_elected {
  ATTEMPTS=50 TIMEOUT=2 MAX_TIMEOUT=4 retry_with_backoff  \
    "[[ \$(curl localhost:6001/status localhost:6002/status | grep leader) ]]"
}

function cleanup {
  local compose_file=$1
  local success=$2
  if [[ "$success" != "true" ]]; then
    echo "Test failure, printing docker-compose logs"
    docker-compose -f "${compose_file}" logs
  fi

  docker-compose -f "${compose_file}" down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}

function initialize_m3_via_coordinator_admin {
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
  curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: override_test_env" 0.0.0.0:7201/api/v1/topic/init -d '{
      "numberOfShards": 64
  }'

  echo "Adding m3coordinator as a consumer to the aggregator publish topic"
  curl -vvvsSf -X POST -H "Topic-Name: aggregated_metrics" -H "Cluster-Environment-Name: override_test_env" 0.0.0.0:7201/api/v1/topic -d '{
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
}