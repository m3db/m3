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
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

DBNODE_HOST=dbnode-cluster-a DBDNODE_PORT=9000 DBNODE_HEALTH_PORT=9002 COORDINATOR_PORT=7201 \
 setup_single_m3db_node

DBNODE_HOST=dbnode-cluster-b DBDNODE_PORT=19000 DBNODE_HEALTH_PORT=19002 COORDINATOR_PORT=17201 \
 setup_single_m3db_node

echo "Start Prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d prometheus-cluster-a
docker-compose -f ${COMPOSE_FILE} up -d prometheus-cluster-b

# Make sure we're proxying writes to the unaggregated namespace in cluster A
echo "Wait until data begins being written to remote storage for the aggregated namespace"
ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace=\"unagg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'

# Make sure we're proxying writes to the unaggregated namespace in cluster B
echo "Wait until data begins being written to remote storage for the aggregated namespace"
ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:19090/api/v1/query?query=database_write_tagged_success\\{namespace=\"unagg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'
