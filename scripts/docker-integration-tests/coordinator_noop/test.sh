#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/coordinator_noop
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
export REVISION

echo "Run coordinator with no etcd"
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes coordinator01

function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

I=0
RES=""
while [[ "$I" -le 5 ]]; do
  # Curling an m3admin API with no etcd client should be a 404.
  RES=$(curl -s -o /dev/null -w '%{http_code}' "localhost:7201/api/v1/services/m3db/placement" || true)
  if [[ "$RES" == "404" ]]; then
    break
  fi
  # Need some time for coordinators to come up.
  sleep 2
  I=$((I+1))
done

if [[ "$RES" != "404" ]]; then
  echo "expected 404 exit code"
  exit 1
fi

QUERY_EXP='{"error":"operation not valid for noop client"}'
RES=$(curl "localhost:7201/api/v1/query_range?start=$(date '+%s')&end=$(date '+%s')&step=10&query=foo")
if [[ "$RES" != "$QUERY_EXP" ]]; then
  echo "Expected resp '$QUERY_EXP', GOT '$RES'"
  exit 1
fi
