#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/query_fanout/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode-cluster-a
docker-compose -f ${COMPOSE_FILE} up -d coordinator-cluster-a

docker-compose -f ${COMPOSE_FILE} up -d dbnode-cluster-b
docker-compose -f ${COMPOSE_FILE} up -d coordinator-cluster-b

# think of this as a defer func() in golang
# function defer {
#   docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
# }
# trap defer EXIT
# exit 0
DBNODE_HOST=dbnode-cluster-a DBDNODE_PORT=9000 DBNODE_HEALTH_PORT=9002 COORDINATOR_PORT=7201 \
 setup_single_m3db_node

DBNODE_HOST=dbnode-cluster-b DBDNODE_PORT=19000 DBNODE_HEALTH_PORT=19002 COORDINATOR_PORT=17201 \
 setup_single_m3db_node

echo "Write data to cluster a"
curl -vvvsS -X POST 0.0.0.0:9003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-a\",endpoint=\"/request\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-a"
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

echo "Write data to cluster b"
curl -vvvsS -X POST 0.0.0.0:19003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-b\",endpoint=\"/request\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-b"
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
