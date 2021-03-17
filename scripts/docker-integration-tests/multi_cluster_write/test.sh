#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH="$M3_PATH"/scripts/docker-integration-tests/multi_cluster_write
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes cluster_a_dbnode01
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes cluster_a_dbnode02
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes cluster_a_coordinator01

docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes cluster_b_dbnode01
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes cluster_b_dbnode02
docker-compose -f ${COMPOSE_FILE} up -d --renew-anon-volumes cluster_b_coordinator01

# Think of this as a defer func() in golang
function defer {
  docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

# Setup cluster A.
DBNODE_ID_01=cluster_a_m3db_local_1 \
DBNODE_ID_02=cluster_a_m3db_local_2 \
DBNODE_HOST_01=cluster_a_dbnode01 \
DBNODE_HOST_02=cluster_a_dbnode02 \
DBNODE_HEALTH_PORT_01=9012 \
DBNODE_HEALTH_PORT_02=9022 \
COORDINATOR_PORT=7201 \
  setup_two_m3db_nodes

# Setup cluster B.
DBNODE_ID_01=cluster_b_m3db_local_1 \
DBNODE_ID_02=cluster_b_m3db_local_2 \
DBNODE_HOST_01=cluster_b_dbnode01 \
DBNODE_HOST_02=cluster_b_dbnode02 \
DBNODE_HEALTH_PORT_01=9112 \
DBNODE_HEALTH_PORT_02=9122 \
COORDINATOR_PORT=17201 \
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

echo "Write data to cluster_a_dbnode01 using cluster (not node) HTTP endpoint"
write_data "agg" "foo" "$(($(date +"%s")))" 12.3456789 9013

# These should pass immediately since it was written to this cluster synchronously.
echo "Expect to read the data back from cluster_a_dbnode01"
read_all "agg" "foo" 1 9012

echo "Expect to read the data back from cluster_a_dbnode02"
read_all "agg" "foo" 1 9022

# These two should eventually succeed once the client asyncronously dual-writes to the
# second cluster.
echo "Wait for the data to become available (via async dual-writing) from cluster_b_dbnode01"
ATTEMPTS=30 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff \
  read_all "agg" "foo" 1 9112

echo "Wait for the data to become available (via async dual-writing) from cluster_b_dbnode02"
ATTEMPTS=30 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff \
  read_all "agg" "foo" 1 9122
