#!/usr/bin/env bash

# Retries a command a configurable number of times with backoff.
#
# The retry count is given by ATTEMPTS (default 3), the initial backoff
# timeout is given by TIMEOUT in seconds (default 1.)
#
# Successive backoffs double the timeout.
# adapted from: https://stackoverflow.com/questions/8350942/how-to-re-run-the-curl-command-automatically-when-the-error-occurs/8351489#8351489
function retry_with_backoff {
  local max_attempts=${ATTEMPTS-5}
  local timeout=${TIMEOUT-1}
  local max_timeout=${MAX_TIMEOUT}
  local attempt=1
  local exitCode=0

  while (( $attempt < $max_attempts ))
  do
    set +e
    eval "$@"
    exitCode=$?
    set -e
    if [[ "$exitCode" == 0 ]]; then
      return 0
    fi

    echo "Failure! Retrying in $timeout.." 1>&2
    sleep $timeout
    attempt=$(( attempt + 1 ))
    timeout=$(( timeout * 2 ))
    if [[ $max_timeout != "" ]]; then
      if [[ $timeout -gt $max_timeout ]]; then
        timeout=$max_timeout
      fi
    fi
  done

  if [[ $exitCode != 0 ]]
  then
    echo "You've failed me for the last time! ($@)" 1>&2
  fi

  return $exitCode
}

function setup_single_m3db_node {
  wait_for_db_init
}

function setup_three_m3db_nodes {
  local dbnode_host_1=${DBNODE_HOST:-dbnode01}
  local dbnode_host_2=${DBNODE_HOST:-dbnode02}
  local dbnode_host_3=${DBNODE_HOST:-dbnode03}
  local dbnode_port=${DBNODE_PORT:-9000}
  local dbnode_host_1_health_port=${DBNODE_HEALTH_PORT:-9012}
  local dbnode_host_2_health_port=${DBNODE_HEALTH_PORT:-9022}
  local dbnode_host_3_health_port=${DBNODE_HEALTH_PORT:-9032}
  local coordinator_port=${COORDINATOR_PORT:-7201}

  echo "Wait for API to be available"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/namespace | jq ".namespaces | length")" == "0" ]'

  echo "Adding placement and agg namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/create -d '{
    "type": "cluster",
    "namespaceName": "agg",
    "retentionTime": "6h",
    "num_shards": 3,
    "replicationFactor": 3,
    "hosts": [
      {
          "id": "m3db_local_1",
          "isolation_group": "rack-a",
          "zone": "embedded",
          "weight": 1024,
          "address": "'"${dbnode_host_1}"'",
          "port": '"${dbnode_port}"'
      },
      {
          "id": "m3db_local_2",
          "isolation_group": "rack-b",
          "zone": "embedded",
          "weight": 1024,
          "address": "'"${dbnode_host_2}"'",
          "port": '"${dbnode_port}"'
      },
      {
          "id": "m3db_local_3",
          "isolation_group": "rack-c",
          "zone": "embedded",
          "weight": 1024,
          "address": "'"${dbnode_host_3}"'",
          "port": '"${dbnode_port}"'
      }
    ]
  }'

  echo "Wait until placement is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/placement | jq .placement.instances.m3db_local_1.id)" == \"m3db_local_1\" ]'

  wait_for_namespaces

  echo "Wait until bootstrapped"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_host_1_health_port}"'/health | jq .bootstrapped)" == true ]'
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_host_2_health_port}"'/health | jq .bootstrapped)" == true ]'
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_host_3_health_port}"'/health | jq .bootstrapped)" == true ]'
}

function wait_for_db_init {
  local dbnode_host=${DBNODE_HOST:-dbnode01}
  local dbnode_port=${DBNODE_PORT:-9000}
  local dbnode_health_port=${DBNODE_HEALTH_PORT:-9002}
  local coordinator_port=${COORDINATOR_PORT:-7201}

  echo "Wait for API to be available"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/namespace | jq ".namespaces | length")" == "0" ]'

  echo "Adding placement and agg namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/create -d '{
    "type": "cluster",
    "namespaceName": "agg",
    "retentionTime": "6h",
    "num_shards": 4,
    "replicationFactor": 1,
    "hosts": [
      {
          "id": "m3db_local",
          "isolation_group": "rack-a",
          "zone": "embedded",
          "weight": 1024,
          "address": "'"${dbnode_host}"'",
          "port": '"${dbnode_port}"'
      }
    ]
  }'

  echo "Wait until placement is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/placement | jq .placement.instances.m3db_local.id)" == \"m3db_local\" ]'

  wait_for_namespaces

  echo "Wait until bootstrapped"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_health_port}"'/health | jq .bootstrapped)" == true ]'
}

function wait_for_namespaces {
  local coordinator_port=${COORDINATOR_PORT:-7201}

  echo "Wait until agg namespace is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/namespace | jq .registry.namespaces.agg.indexOptions.enabled)" == true ]'

  echo "Adding unagg namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/namespace/create -d '{
    "namespaceName": "unagg",
    "retentionTime": "6h"
  }'

  echo "Wait until unagg namespace is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/namespace | jq .registry.namespaces.unagg.indexOptions.enabled)" == true ]'

  echo "Adding coldWritesRepairAndNoIndex namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/services/m3db/namespace -d '{
    "name": "coldWritesRepairAndNoIndex",
    "options": {
      "bootstrapEnabled": true,
      "flushEnabled": true,
      "writesToCommitLog": true,
      "cleanupEnabled": true,
      "snapshotEnabled": true,
      "repairEnabled": true,
      "coldWritesEnabled": true,
      "retentionOptions": {
        "retentionPeriodDuration": "4h",
        "blockSizeDuration": "1h",
        "bufferFutureDuration": "10m",
        "bufferPastDuration": "10m",
        "blockDataExpiry": true,
        "blockDataExpiryAfterNotAccessPeriodDuration": "5m"
      }
    }
  }'

  echo "Wait until coldWritesRepairAndNoIndex namespace is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/namespace | jq .registry.namespaces.coldWritesRepairAndNoIndex.coldWritesEnabled)" == true ]'
}

