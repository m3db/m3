#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH="$M3_PATH"/scripts/docker-integration-tests/repair
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes dbnode01
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes dbnode02
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes coordinator01

# Think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

setup_two_m3db_nodes

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

# Write 2 block sizes into the past to ensure it's a repairable block since the current mutable
# block will not be repaired. Use the node-specific port to ensure the write only goes to dbnode01
# and not the other two nodes.
echo "Write data for 'now - 2 * blockSize' to dbnode01"
write_data "coldWritesRepairAndNoIndex" "foo" "$(($(date +"%s") - 60 * 60 * 2))" 12.3456789 9012

# This should pass immediately since it was written to this node.
echo "Expect to read the data back from dbnode01"
read_all "coldWritesRepairAndNoIndex" "foo" 1 9012

# This should eventually succeed once a repair detects the mismatch.
echo "Wait for the data to become available (via repairs) from dbnode02"
ATTEMPTS=30 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff \
  read_all "coldWritesRepairAndNoIndex" "foo" 1 9022
