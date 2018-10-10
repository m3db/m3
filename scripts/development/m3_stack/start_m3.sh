#!/usr/bin/env bash

set -xe

DOCKER_ARGS="-d --renew-anon-volumes"
if [[ "$FORCE_BUILD" = true ]] ; then
    DOCKER_ARGS="--build -d --renew-anon-volumes"
fi

echo "Bringing up nodes in the backgorund with docker compose, remember to run ./stop.sh when done"
docker-compose -f docker-compose.yml up $DOCKER_ARGS m3coordinator01
docker-compose -f docker-compose.yml up $DOCKER_ARGS m3db_seed
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
    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3aggregator01
    docker-compose -f docker-compose.yml up $DOCKER_ARGS m3collector01
else
    echo "Not running aggregator pipeline"
fi


echo "Sleeping to wait for nodes to initialize"
sleep 10

echo "Nodes online"

echo "Initializing namespace"
curl -vvvsSf -X POST localhost:7201/api/v1/namespace -d '{
  "name": "prometheus_metrics",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodNanos": 172800000000000,
      "blockSizeNanos": 7200000000000,
      "bufferFutureNanos": 600000000000,
      "bufferPastNanos": 600000000000,
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessPeriodNanos": 300000000000
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeNanos": 7200000000000
    }
  }
}'
echo "Done initializing namespace"

echo "Validating namespace"
[ "$(curl -sSf localhost:7201/api/v1/namespace | jq .registry.namespaces.prometheus_metrics.indexOptions.enabled)" == true ]
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
echo "Done initializing topology"

if [[ "$AGGREGATOR_PIPELINE" = true ]]; then
    curl -vvvsSf -X POST localhost:7201/api/v1/services/m3aggregator/placement/init -d '{
        "num_shards": 64,
        "replication_factor": 1,
        "instances": [
            {
                "id": "m3aggregator01",
                "isolation_group": "rack-a",
                "zone": "embedded",
                "weight": 1024,
                "endpoint": "m3aggregator01:6000",
                "hostname": "m3aggregator01",
                "port": 6000
            }
        ]
    }'
fi

echo "Validating topology"
[ "$(curl -sSf localhost:7201/api/v1/placement | jq .placement.instances.m3db_seed.id)" == '"m3db_seed"' ]
echo "Done validating topology"

echo "Prometheus available at localhost:9090"
echo "Grafana available at localhost:3000"
echo "Run ./stop.sh to shutdown nodes when done"
