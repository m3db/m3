#!/bin/bash
# Sets up placement + an initial namespace for M3DB. Copied from start_m3.sh; not really
# intended for landing.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source "${DIR}/../../../../scripts/docker-integration-tests/common.sh"

echo "Initializing namespaces"
curl -vvvsSf -X POST localhost:7201/api/v1/namespace -d '{
  "name": "ns1",
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
[ "$(curl -sSf localhost:7201/api/v1/namespace | jq '.registry|.namespaces|."ns1"|.indexOptions|.enabled')" == true ]
echo "Done validating namespace"

echo "Initializing topology"
curl -vvvsSf -X POST localhost:7201/api/v1/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 1,
    "instances": [
        {
            "id": "node1",
            "isolation_group": "rack-a",
            "zone": "embedded",
            "weight": 1024,
            "endpoint": "127.0.0.1:9000",
            "hostname": "127.0.0.1",
            "port": 9000
        }
    ]
}'

echo "Validating topology"
[ "$(curl -sSf localhost:7201/api/v1/placement | jq .placement.instances.node1.id)" == '"node1"' ]
echo "Done validating topology"

echo "Waiting until shards are marked as available"
ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | grep -c INITIALIZING)" -eq 0 ]'

echo "All shards available; DB is setup"