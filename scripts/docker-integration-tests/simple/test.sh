#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)

echo "Run m3dbnode docker container"

CONTAINER_NAME="m3dbnode-version-${REVISION}"
docker create --name "${CONTAINER_NAME}" -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 -p 7201:7201 "m3dbnode_integration:${REVISION}"

# think of this as a defer func() in golang
function defer {
  echo "Test complete, dumping logs"
  echo "---------------------------"
  docker logs "${CONTAINER_NAME}"
  echo "---------------------------"

  echo "Removing docker container"
  docker rm --force "${CONTAINER_NAME}"
}
trap defer EXIT

docker start "${CONTAINER_NAME}"

# TODO(rartoul): Rewrite this test to use a docker-compose file like the others so that we can share all the
# DB initialization logic with the setup_single_m3db_node command in common.sh like the other files. Right now
# we can't do that because this test doesn't use the docker-compose networking so we have to specify 127.0.0.1
# as the endpoint in the placement instead of being able to use dbnode01.
echo "Wait for DB to be up"
ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
  'curl -vvvsSf 0.0.0.0:9002/bootstrappedinplacementornoplacement'

echo "Wait for coordinator API to be up"
ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
  'curl -vvvsSf 0.0.0.0:7201/health'

echo "Adding namespace"
curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/services/m3db/namespace -d '{
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

echo "Sleep until namespace is init'd"
ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/services/m3db/namespace | jq .registry.namespaces.agg.indexOptions.enabled)" == true ]'

curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/services/m3db/namespace -d '{
  "name": "unagg",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodDuration": "8h",
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
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/services/m3db/namespace | jq .registry.namespaces.unagg.indexOptions.enabled)" == true ]'

echo "Placement initialization"
curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/services/m3db/placement/init -d '{
    "num_shards": 4,
    "replication_factor": 1,
    "instances": [
        {
            "id": "m3db_local",
            "isolation_group": "rack-a",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "127.0.0.1:9000",
            "hostname": "127.0.0.1",
            "port": 9000
        }
    ]
}'

echo "Sleep until placement is init'd"
ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/services/m3db/placement | jq .placement.instances.m3db_local.id)" == \"m3db_local\" ]'

echo "Sleep until bootstrapped"
ATTEMPTS=7 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:9002/health | jq .bootstrapped)" == true ]'

echo "Waiting until shards are marked as available"
ATTEMPTS=10 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/services/m3db/placement | grep -c INITIALIZING)" -eq 0 ]'

echo "Write data"
curl -vvvsS -X POST 0.0.0.0:9003/writetagged -d '{
  "namespace": "unagg",
  "id": "foo",
  "tags": [
    {
      "name": "city",
      "value": "new_york"
    },
    {
      "name": "endpoint",
      "value": "/request"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

echo "Read data"
ATTEMPTS=3 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf -X POST 0.0.0.0:9003/query -d "{
    \"namespace\": \"unagg\",
    \"query\": {
      \"regexp\": {
        \"field\": \"city\",
        \"regexp\": \".*\"
      }
    },
    \"rangeStart\": 0,
    \"rangeEnd\":'\"$(date +\"%s\")\"'
  }" | jq ".results | length")" == "1" ]'

echo "Deleting placement"
curl -vvvsSf -X DELETE 0.0.0.0:7201/api/v1/services/m3db/placement

echo "Deleting namespace"
curl -vvvsSf -X DELETE 0.0.0.0:7201/api/v1/services/m3db/namespace/unagg
