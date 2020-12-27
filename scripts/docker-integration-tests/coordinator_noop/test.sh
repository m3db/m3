#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/coordinator_noop
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
export REVISION

echo "Run coordinator with no etcd"
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes coordinator01
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes etcd01

function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

I=0
RES=""
while [[ "$I" -le 5 ]]; do
  if curl -vvvsSf -X POST localhost:7201/api/v1/services/m3coordinator/placement/init -d '{
    "instances": [
        {
            "id": "m3coordinator01",
            "zone": "embedded",
            "endpoint": "m3coordinator01:7507",
            "hostname": "m3coordinator01",
            "port": 7507
        }
    ]
  }'; then
    break
  fi
  # Need some time for coordinators to come up.
  sleep 2
  I=$((I+1))
done

if ! curl -vvvsSf localhost:7201/api/v1/services/m3coordinator/placement; then
  echo "could not fetch existing placement"
  exit 1
fi

QUERY_EXP='{"status":"error","error":"operation not valid for noop client"}'
RES=$(curl "localhost:7201/m3query/api/v1/query_range?start=$(date '+%s')&end=$(date '+%s')&step=10&query=foo")
if [[ "$RES" != "$QUERY_EXP" ]]; then
  echo "Expected resp '$QUERY_EXP', GOT '$RES'"
  exit 1
fi
