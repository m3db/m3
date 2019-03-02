#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/prometheus/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d dbnode01
docker-compose -f ${COMPOSE_FILE} up -d coordinator01

# think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_single_m3db_node

echo "Start Prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d prometheus01

# Ensure Prometheus can proxy a Prometheus query
echo "Wait until the remote write endpoint generates and allows for data to be queried"
ATTEMPTS=6 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=prometheus_remote_storage_succeeded_samples_total | jq -r .data.result[].value[1]) -gt 100 ]]'

# Make sure we're proxying writes to the unaggregated namespace
echo "Wait until data begins being written to remote storage for the unaggregated namespace"
ATTEMPTS=6 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace=\"unagg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'

# Make sure we're proxying writes to the aggregated namespace
echo "Wait until data begins being written to remote storage for the aggregated namespace"
ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
  '[[ $(curl -sSf 0.0.0.0:9090/api/v1/query?query=database_write_tagged_success\\{namespace=\"agg\"\\} | jq -r .data.result[0].value[1]) -gt 0 ]]'

