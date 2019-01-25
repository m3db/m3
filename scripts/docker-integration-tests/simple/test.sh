#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)

echo "Run m3dbnode docker container"

CONTAINER_NAME="m3dbnode-version-${REVISION}"
docker create --name "${CONTAINER_NAME}" -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 -p 7201:7201 "m3dbnode_integration:${REVISION}"

# think of this as a defer func() in golang
# function defer {
#   echo "Remove docker container"
#   docker rm --force "${CONTAINER_NAME}"
# }
# trap defer EXIT

docker start "${CONTAINER_NAME}"
if [ $? -ne 0 ]; then
  echo "m3dbnode docker failed to start"
  docker logs "${CONTAINER_NAME}"
  exit 1
fi

setup_single_m3db_node

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
queryResult=$(curl -sSf -X POST 0.0.0.0:9003/query -d '{
  "namespace": "unagg",
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
curl -vvvsSf -X DELETE 0.0.0.0:7201/api/v1/namespace/unagg
