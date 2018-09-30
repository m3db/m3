#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/prometheus/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

echo "Sleeping for a bit to ensure db up"
sleep 15 # TODO Replace sleeps with logic to determine when to proceed

echo "Adding namespace"
curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/namespace -d '{
  "name": "agg",
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

echo "Sleep until namespace is init'd"
ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.agg.indexOptions.enabled)" == true ]'

curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/namespace -d '{
  "name": "unagg",
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

echo "Sleep until namespace is init'd"
ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.unagg.indexOptions.enabled)" == true ]'

echo "Placement initialization"
curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/placement/init -d '{
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

echo "Sleep until placement is init'd"
ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | jq .placement.instances.m3db_local.id)" == \"m3db_local\" ]'

echo "Sleep until bootstrapped"
ATTEMPTS=6 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:9002/health | jq .bootstrapped)" == true ]'

echo "Start Prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d prometheus01

# Ensure Prometheus can proxy a Prometheus query
echo "Wait until the remote write endpoint generates and allows for data to be queried"
ATTEMPTS=6 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=prometheus_remote_storage_succeeded_samples_total | jq -r .data.result[].value[1]) -gt 100 ]]'

# Make sure we're proxying writes to the unaggregated namespace
echo "Wait until data beings being written to remote storage for the unaggregated namespace"
ATTEMPTS=6 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace="unagg"\\} | jq .data.result[0].value[1]) -gt 0 ]]'

# Make sure we're proxying writes to the aggregated namespace
echo "Wait until data beings being written to remote storage for the aggregated namespace"
ATTEMPTS=6 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace="agg"\\} | jq .data.result[0].value[1]) -gt 0 ]]'

docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
