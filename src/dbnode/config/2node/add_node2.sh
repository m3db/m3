#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

source "${DIR}/../../../../scripts/docker-integration-tests/common.sh"

curl -X POST localhost:7201/api/v1/services/m3db/placement -d '{
  "instances": [
    {
      "id": "node2",
      "isolationGroup": "group2",
      "zone": "embedded",
      "weight": 1024,
      "endpoint": "localhost:10000",
      "hostname": "localhost",
      "port": 10000
    }
  ]
}'

echo "Waiting until shards are marked as available (node2 is bootstrapped)"
ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | grep -c INITIALIZING)" -eq 0 ]'

echo "All shards available; node2 is in the placement"