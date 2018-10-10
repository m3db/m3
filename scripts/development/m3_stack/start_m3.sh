#!/usr/bin/env bash

set -xe

echo "Bringing up nodes in the backgorund with docker compose, remember to run ./stop.sh when done"
docker-compose -f docker-compose.yml up -d --renew-anon-volumes
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

echo "Initializing M3DB topology"
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
echo "Done initializing M3DB topology"

echo "Validating topology"
[ "$(curl -sSf localhost:7201/api/v1/placement | jq .placement.instances.m3db_seed.id)" == '"m3db_seed"' ]
echo "Done validating topology"

echo "Initializing M3Coordinator topology"
curl -vvvsSf -X POST localhost:7201/api/v1/services/m3coordinator/placement/init -d '{
    "instances": [
        {
            "id": "coordinator01",
            "zone": "embedded",
            "endpoint": "coordinator01:7507",
            "hostname": "coordinator01",
            "port": 7507
        }
    ]
}'
echo "Done initializing M3Coordinator topology"

echo "Validating M3Coordinator topology"
[ "$(curl -sSf localhost:7201/api/v1/services/m3coordinator/placement | jq .placement.instances.coordinator01.id)" == '"coordinator01"' ]
echo "Done validating topology"

echo "Prometheus available at localhost:9090"
echo "Grafana available at localhost:3000"
echo "Run ./stop.sh to shutdown nodes when done"