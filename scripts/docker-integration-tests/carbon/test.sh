#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/carbon/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

# think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT


function setup_single_m3db_node_with_long_ns {
  echo "Wait for API to be available"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq ".namespaces | length")" == "0" ]'

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
        "retentionPeriodDuration": "48h",
        "blockSizeDuration": "2h",
        "bufferFutureDuration": "10m",
        "bufferPastDuration": "10m",
        "blockDataExpiry": true,
        "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
      },
      "indexOptions": {
        "enabled": true,
        "blockSizeDuration": "2h"
      }
    }
  }'

  echo "Wait until namespace is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.agg.indexOptions.enabled)" == true ]'

  echo "Adding namespace"
  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/namespace -d '{
    "name": "agglong",
    "options": {
      "bootstrapEnabled": true,
      "flushEnabled": true,
      "writesToCommitLog": true,
      "cleanupEnabled": true,
      "snapshotEnabled": true,
      "repairEnabled": false,
      "retentionOptions": {
        "retentionPeriodDuration": "48h",
        "blockSizeDuration": "2h",
        "bufferFutureDuration": "10m",
        "bufferPastDuration": "10m",
        "blockDataExpiry": true,
        "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
      },
      "indexOptions": {
        "enabled": true,
        "blockSizeDuration": "2h"
      }
    }
  }'

  echo "Wait until namespace is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.agglong.indexOptions.enabled)" == true ]'

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
        "retentionPeriodDuration": "48h",
        "blockSizeDuration": "2h",
        "bufferFutureDuration": "10m",
        "bufferPastDuration": "10m",
        "blockDataExpiry": true,
        "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
      },
      "indexOptions": {
        "enabled": true,
        "blockSizeDuration": "2h"
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
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:9002/health | jq .bootstrapped)" == true ]'

  echo "Waiting until shards are marked as available"
  ATTEMPTS=10 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | grep -c INITIALIZING)" -eq 0 ]'
}

setup_single_m3db_node_with_long_ns

function read_carbon {
  target=$1
  expected_val=$2
  end=$(date +%s)
  start=$(($end-10))
  RESPONSE=$(curl -sSfg "http://localhost:7201/api/v1/graphite/render?target=$target&from=$start&until=$end")
  test "$(echo "$RESPONSE" | jq ".[0].datapoints | .[][0] | select(. != null)" | tail -n 1)" = "$expected_val"
  return $?
}

echo "Writing out a carbon metric that should use a min aggregation"
t=$(date +%s)
# 41 should win out here because min(42,41) == 41. Note that technically this test could
# behave incorrectly if the values end up in separate flushes due to the bufferPast
# configuration of the downsampler, but we set reasonable values for bufferPast so that
# should not happen.
echo "foo.min.aggregate.baz 41 $t" | nc 0.0.0.0 7204
echo "foo.min.aggregate.baz 42 $t" | nc 0.0.0.0 7204
echo "Attempting to read min aggregated carbon metric"
ATTEMPTS=5 TIMEOUT=1 retry_with_backoff read_carbon foo.min.aggregate.baz 41

echo "Writing out a carbon metric that should not be aggregated"
t=$(date +%s)
# 43 should win out here because of M3DB's upsert semantics. While M3DB's upsert
# semantics are not always guaranteed, it is guaranteed for a minimum time window
# that is as large as bufferPast/bufferFuture which should be much more than enough
# time for these two commands to complete.
echo "foo.min.already-aggregated.baz 42 $t" | nc 0.0.0.0 7204
echo "foo.min.already-aggregated.baz 43 $t" | nc 0.0.0.0 7204
echo "Attempting to read unaggregated carbon metric"
ATTEMPTS=5 TIMEOUT=1 retry_with_backoff read_carbon foo.min.already-aggregated.baz 43

echo "Writing out a carbon metric that should should use the default mean aggregation"
t=$(date +%s)
# Mean of 10 and 20 is 15. Same comment as the min aggregation test above.
echo "foo.min.catch-all.baz 10 $t" | nc 0.0.0.0 7204
echo "foo.min.catch-all.baz 20 $t" | nc 0.0.0.0 7204
echo "Attempting to read mean aggregated carbon metric"


echo "Writing out a carbon metric to a longer retention namespace should appear"
t=$(date +%s)
# Mean of 10 and 20 is 15. Same comment as the min aggregation test above.
echo "foo.min.bar.long 10 $t" | nc 0.0.0.0 7204
echo "foo.min.bar.long 20 $t" | nc 0.0.0.0 7204
echo "Attempting to read mean aggregated carbon metric"
ATTEMPTS=5 TIMEOUT=1 retry_with_backoff read_carbon foo.min.bar.long 15

