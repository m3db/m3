#!/usr/bin/env bash

set -xe

rm -rf /tmp/m3dbdata/
mkdir -p /tmp/m3dbdata/

echo "Build docker images"
docker-compose -f docker-compose.yml build

echo "Run m3dbnode and m3coordinator containers"

docker-compose -f docker-compose.yml up -d dbnode01
docker-compose -f docker-compose.yml up -d coordinator01

echo "Sleeping for a bit to ensure db up"

sleep 10 # TODO Replace sleeps with logic to determine when to proceed

echo "Adding namespace"

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

echo "Sleep while namespace is init'd"

sleep 10 # TODO Replace sleeps with logic to determine when to proceed

[ "$(curl -sSf localhost:7201/api/v1/namespace | jq .registry.namespaces.prometheus_metrics.indexOptions.enabled)" == true ]

echo "Initialization placement"

curl -vvvsSf -X POST localhost:7201/api/v1/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 1,
    "instances": [
        {
            "id": "m3db_local",
            "isolation_group": "rack-a",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "dbnode01:9000",
            "hostname": "dbnode01",
            "port": 9000
        }
    ]
}'

[ "$(curl -sSf localhost:7201/api/v1/placement | jq .placement.instances.m3db_local.id)" == '"m3db_local"' ]

echo "Wait for placement to fully initialize"

echo "Start Prometheus containers"

docker-compose -f docker-compose.yml up -d prometheus01

echo "Sleep for 30 seconds to let the remote write endpoint generate some data"

sleep 30

# Ensure Prometheus can proxy a Prometheus query
[ "$(curl -sSf localhost:9090/api/v1/query?query=prometheus_remote_storage_succeeded_samples_total | jq .data.result[].value[1])" != '"0"' ]

docker-compose -f docker-compose.yml down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
