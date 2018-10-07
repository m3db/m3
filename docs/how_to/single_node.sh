#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh

REVISION=$(git rev-parse HEAD)

CONTAINER_NAME="m3dbnode-version-${REVISION}"

# think of this as a defer func() in golang
function defer {
  echo "Remove docker container"
  docker rm --force "${CONTAINER_NAME}"
}
trap defer EXIT


docker pull quay.io/m3/m3dbnode:latest
docker create --name "${CONTAINER_NAME}" -p 7201:7201 -p 9003:9003 -p 9002:9002 quay.io/m3/m3dbnode:latest
docker start "${CONTAINER_NAME}"
if [ $? -ne 0 ]; then
  echo "m3dbnode docker failed to start"
  docker logs "${CONTAINER_NAME}"
  exit 1
fi

sleep 5
echo "checking uptime of coordinator service"
ATTEMPTS=6 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/health | jq .uptime)" ]'

curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "48h"
}'

echo "check placement info"
ATTEMPTS=6 TIMEOUT=1 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | jq .placement.instances.m3db_local.id)" == \"m3db_local\" ]'

echo "sleep until bootstrapped"
ATTEMPTS=6 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:9002/health | jq .bootstrapped)" == true ]'

curl -vvvsS -X POST http://localhost:9003/writetagged -d '{
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
queryResult=$(curl -sSf -X POST localhost:9003/query -d '{
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
