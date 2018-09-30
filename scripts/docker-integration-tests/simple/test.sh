#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)

echo "Run m3dbnode docker container"

CONTAINER_NAME="m3dbnode-version-${REVISION}"
docker create --name "${CONTAINER_NAME}" -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 -p 7201:7201 "m3dbnode_integration:${REVISION}"

# think of this as a defer func() in golang
function defer {
  echo "Remove docker container"
  docker rm --force "${CONTAINER_NAME}"
}
trap defer EXIT

docker start "${CONTAINER_NAME}"
if [ $? -ne 0 ]; then
  echo "m3dbnode docker failed to start"
  docker logs "${CONTAINER_NAME}"
  exit 1
fi

echo "Sleeping for a bit to ensure db up"
sleep 5 # TODO Replace sleeps with logic to determine when to proceed

echo "Adding namespace"
curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/namespace -d '{
  "name": "default",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": false,
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
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.default.indexOptions.enabled)" == true ]'

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
            "endpoint": "127.0.0.1:9000",
            "hostname": "127.0.0.1",
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

echo "Write data"
curl -vvvsS -X POST 0.0.0.0:9003/writetagged -d '{
  "namespace": "default",
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
queryResult=$(curl -sSf -X POST 0.0.0.0:9003/query -d '{
  "namespace": "default",
  "query": {
    "regexp": {
      "field": "city",
      "regexp": ".*"
    }
  },
  "rangeStart": 0,
  "rangeEnd":'"$(date +"%s")"'
}' | jq '.results | length')

if [ "$queryResult" -lt 1 ]; then
  echo "Result not found"
  exit 1
else
  echo "Result found"
fi

echo "Deleting placement"
curl -vvvsSf -X DELETE 0.0.0.0:7201/api/v1/placement

echo "Deleting namespace"
curl -vvvsSf -X DELETE 0.0.0.0:7201/api/v1/namespace/default