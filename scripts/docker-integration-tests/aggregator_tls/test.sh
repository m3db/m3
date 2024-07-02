#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE="$M3_PATH"/scripts/docker-integration-tests/aggregator_tls/docker-compose.yml
export REVISION

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

echo "Initializing m3msg topic for m3coordinator ingestion from m3aggregators"
curl -vvvsSf -X POST -H "Cluster-Environment-Name: override_test_env" localhost:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'

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
echo "Adding m3coordinator as a consumer to the aggregator topic"
curl -vvvsSf -X POST -H "Cluster-Environment-Name: override_test_env" localhost:7201/api/v1/topic -d '{
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
  test "$(echo "$RESPONSE" | jq ".[0].datapoints | .[][0] | select(. != null)" | tail -n 1)" = "$expected_val"
  return $?
}

# Send metric values 40 and 44 every second
echo "Sending unaggregated carbon metrics to m3coordinator"
bash -c 'while true; do t=$(date +%s); echo "foo.bar.baz 40 $t" | nc -q 0 0.0.0.0 7204; echo "foo.bar.baz 44 $t" | nc -q 0 0.0.0.0 7204; sleep 1; done' &

# Track PID to kill on exit
METRIC_EMIT_PID="$!"

# Read back the averaged averaged metric, we configured graphite
# aggregation policy to average each tile and we are emitting
# values 40 and 44 to get an average of 42 each tile
echo "Read back aggregated averaged metric"
ATTEMPTS=10 TIMEOUT=1 retry_with_backoff read_carbon foo.bar.* 42
