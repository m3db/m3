#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh

docker pull quay.io/m3/m3dbnode:latest
docker run -p 7201:7201 -p 9003:9003 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db quay.io/m3/m3dbnode:latest

sleep 15
echo "checking uptime of coordinator service"
ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/health | jq 'has("uptime")')" == true ]'

curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "48h"
}'

sleep 15
echo "check placement info"
ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl http://localhost:7201/api/v1/placement | jq .placement.instances.m3db_local.id)" == "m3db_local" ]'

curl -sSf -X POST http://localhost:9003/writetagged -d '{
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
    "timestamp": '"$(date "+%s")"',
    "value": 42.123456789
  }
}
'

curl -sSf -X POST http://localhost:9003/query -d '{
  "namespace": "default",
  "query": {
    "regexp": {
      "field": "city",
      "regexp": ".*"
    }
  },
  "rangeStart": 0,
  "rangeEnd": '"$(date "+%s")"'
}' | jq .

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
