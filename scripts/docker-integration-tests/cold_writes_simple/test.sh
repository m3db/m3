#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH="$M3_PATH"/scripts/docker-integration-tests/cold_writes_simple
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes dbnode01
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes coordinator01

# Think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_single_m3db_node

function write_data {
  namespace=$1
  id=$2
  timestamp=$3
  value=$4

  respCode=$(curl -s -o /dev/null -X POST -w "%{http_code}" 0.0.0.0:9003/write -d '{
    "namespace": "'"$namespace"'",
    "id": "'"$id"'",
    "datapoint": {
      "timestamp":'"$timestamp"',
      "value": '"$value"'
    }
  }')

  if [[ $respCode -eq "200" ]]; then
    return 0
  else
    return 1
  fi
}

function read_all {
  namespace=$1
  id=$2
  expected_datapoints=$3

  received_datapoints=$(curl -sSf -X POST 0.0.0.0:9003/fetch -d '{
    "namespace": "'"$namespace"'",
    "id": "'"$id"'",
    "rangeStart": 0,
    "rangeEnd":'"$(date +"%s")"'
  }' | jq '.datapoints | length')

  if [[ $expected_datapoints -eq $received_datapoints ]]; then
    return 0
  else
    return 1
  fi
}

echo "Write data for 'now - 2 * bufferPast' (testing cold writes from memory)"
write_data "coldWritesRepairAndNoIndex" "foo" "$(($(date +"%s") - 60 * 10 * 2))" 12.3456789

echo "Expect to read 1 datapoint"
read_all "coldWritesRepairAndNoIndex" "foo" 1

echo "Write data for 'now - 2 * blockSize' (testing compaction to disk)"
write_data "coldWritesRepairAndNoIndex" "foo" "$(($(date +"%s") - 60 * 60 * 2))" 98.7654321

echo "Wait until cold writes are flushed"
ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
  '[ -n "$(docker-compose -f ${COMPOSE_FILE} exec dbnode01 find /var/lib/m3db/data/coldWritesRepairAndNoIndex -name "*1-checkpoint.db")" ]'

echo "Restart DB (test bootstrapping cold writes)"
docker-compose -f ${COMPOSE_FILE} restart dbnode01

echo "Wait until bootstrapped"
ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
  '[ "$(curl -sSf 0.0.0.0:9002/health | jq .bootstrapped)" == true ]'

echo "Expect to read 2 datapoints"
read_all "coldWritesRepairAndNoIndex" "foo" 2
