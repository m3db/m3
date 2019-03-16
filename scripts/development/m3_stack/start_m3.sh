#!/usr/bin/env bash

set -xe

source "$(pwd)/../../docker-integration-tests/common.sh"

DOCKER_ARGS="-d --renew-anon-volumes"
if [[ "$FORCE_BUILD" = true ]] ; then
    DOCKER_ARGS="--build -d --renew-anon-volumes"
fi

echo "Bringing up nodes in the background with docker compose, remember to run ./stop.sh when done"
docker-compose -f docker-compose.yml up $DOCKER_ARGS m3coordinator01
docker-compose -f docker-compose.yml up --build -d --renew-anon-volumes m3db_seed
docker-compose -f docker-compose.yml up $DOCKER_ARGS prometheus01
docker-compose -f docker-compose.yml up $DOCKER_ARGS grafana

if [[ "$MULTI_DB_NODE" = true ]] ; then
    echo "Running multi node"
    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3db_data01
    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3db_data02
else
    echo "Running single node"
fi

if [[ "$AGGREGATOR_PIPELINE" = true ]]; then
    echo "Running aggregator pipeline"
    curl -vvvsSf -X POST localhost:7201/api/v1/services/m3aggregator/placement/init -d '{
        "num_shards": 64,
        "replication_factor": 1,
        "instances": [
            {
                "id": "m3aggregator01:6000",
                "isolation_group": "rack-a",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3aggregator01:6000",
                "hostname": "m3aggregator01",
                "port": 6000
            }
        ]
    }'

    echo "Initializing m3msg topic for ingestion"
    curl -vvvsSf -X POST localhost:7201/api/v1/topic/init -d '{
        "numberOfShards": 64
    }'

    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3aggregator01
    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3collector01
else
    echo "Not running aggregator pipeline"
fi


echo "Sleeping to wait for nodes to initialize"
sleep 10

echo "Nodes online"

echo "Initializing namespaces"
curl -vvvsSf -X POST localhost:7201/api/v1/namespace -d '{
  "name": "metrics_0_30m",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodDuration": "30m",
      "blockSizeDuration": "10m",
      "bufferFutureDuration": "5m",
      "bufferPastDuration": "5m",
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeDuration": "10m"
    }
  }
}'
curl -vvvsSf -X POST localhost:7201/api/v1/namespace -d '{
  "name": "metrics_10s_48h",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodDuration": "48h",
      "blockSizeDuration": "4h",
      "bufferFutureDuration": "10m",
      "bufferPastDuration": "10m",
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeDuration": "4h"
    }
  }
}'
echo "Done initializing namespaces"

echo "Validating namespace"
[ "$(curl -sSf localhost:7201/api/v1/namespace | jq .registry.namespaces.metrics_0_30m.indexOptions.enabled)" == true ]
[ "$(curl -sSf localhost:7201/api/v1/namespace | jq .registry.namespaces.metrics_10s_48h.indexOptions.enabled)" == true ]
echo "Done validating namespace"

echo "Initializing topology"
if [[ "$MULTI_DB_NODE" = true ]] ; then
    curl -vvvsSf -X POST localhost:7201/api/v1/placement/init -d '{
        "num_shards": 64,
        "replication_factor": 3,
        "instances": [
            {
                "id": "m3db_seed",
                "isolation_group": "rack-a",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_seed:9000",
                "hostname": "m3db_seed",
                "port": 9000
            },
            {
                "id": "m3db_data01",
                "isolation_group": "rack-b",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_data01:9000",
                "hostname": "m3db_data01",
                "port": 9000
            },
            {
                "id": "m3db_data02",
                "isolation_group": "rack-c",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_data02:9000",
                "hostname": "m3db_data02",
                "port": 9000
            }
        ]
    }'
else
    curl -vvvsSf -X POST localhost:7201/api/v1/placement/init -d '{
        "num_shards": 64,
        "replication_factor": 1,
        "instances": [
            {
                "id": "m3db_seed",
                "isolation_group": "rack-a",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3db_seed:9000",
                "hostname": "m3db_seed",
                "port": 9000
            }
        ]
    }'
fi

echo "Validating topology"
[ "$(curl -sSf localhost:7201/api/v1/placement | jq .placement.instances.m3db_seed.id)" == '"m3db_seed"' ]
echo "Done validating topology"

echo "Waiting until shards are marked as available"
ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | grep -c INITIALIZING)" -eq 0 ]'

if [[ "$AGGREGATOR_PIPELINE" = true ]]; then
    echo "Initializing M3Coordinator topology"
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
    echo "Done initializing M3Coordinator topology"

    echo "Validating M3Coordinator topology"
    [ "$(curl -sSf localhost:7201/api/v1/services/m3coordinator/placement | jq .placement.instances.m3coordinator01.id)" == '"m3coordinator01"' ]
    echo "Done validating topology"

    # Do this after placement for m3coordinator is created.
    echo "Adding m3coordinator as a consumer to the topic"
    curl -vvvsSf -X POST localhost:7201/api/v1/topic -d '{
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

    # May not necessarily flush
    echo "Sending unaggregated metric to m3collector"
    curl http://localhost:7206/api/v1/json/report -X POST -d '{"metrics":[{"type":"gauge","value":42,"tags":{"__name__":"foo_metric","foo":"bar"}}]}'
fi

echo "Prometheus available at localhost:9090"
echo "Grafana available at localhost:3000"
echo "Run ./stop.sh to shutdown nodes when done"
