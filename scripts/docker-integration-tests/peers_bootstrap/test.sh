#!/usr/bin/env bash

set -xe

source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH=$GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/peers_bootstrap
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes dbnode01
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes dbnode02
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes dbnode03
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes coordinator01

# Think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_three_m3db_nodes

function write_data {
  namespace=$1
  id=$2
  timestamp=$3
  value=$4
  port=$5

  respCode=$(curl -s -o /dev/null -X POST -w "%{http_code}" 0.0.0.0:"$port"/write -d '{
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
  port=$4

  received_datapoints=$(curl -sSf -X POST 0.0.0.0:"$port"/fetch -d '{
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

# echo "Stopping dbnode03"
# docker-compose -f ${COMPOSE_FILE} stop dbnode03

# echo "Write data to dbnode01 and dbnode02"
# TS="$(date +"%s")"
# write_data "coldWritesRepairAndNoIndex" "foo" "$TS" 123 9012
# write_data "coldWritesRepairAndNoIndex" "foo" "$TS" 123 9022

# echo "Starting dbnode03"
# docker-compose -f ${COMPOSE_FILE} start dbnode03
# echo "Waiting for dbnode03 to be bootstrapped"
# ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
#     '[ "$(curl -sSf 0.0.0.0:9032/health | jq .bootstrapped)" == true ]'

# echo "Expect to read the data back from dbnode03"
# read_all "coldWritesRepairAndNoIndex" "foo" 1 9032

echo "Stopping dbnode02 and dbnode03"
docker-compose -f ${COMPOSE_FILE} stop dbnode02
docker-compose -f ${COMPOSE_FILE} stop dbnode03

echo "Write data to dbnode01"
write_data "coldWritesRepairAndNoIndex" "bar" "$(date +"%s")" 123 9012

echo "Starting dbnode02 and dbnode03"
docker-compose -f ${COMPOSE_FILE} start dbnode02
docker-compose -f ${COMPOSE_FILE} start dbnode03

echo "Waiting for dbnode02 and dbnode03 to be bootstrapped"
ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:9022/health | jq .bootstrapped)" == true ]'
ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:9032/health | jq .bootstrapped)" == true ]'

echo "Expect to read the data back from dbnode02 and dbnode03"
read_all "coldWritesRepairAndNoIndex" "bar" 1 9022
read_all "coldWritesRepairAndNoIndex" "bar" 1 9032
