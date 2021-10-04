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

function setup_single_m3db_node_long_namespaces {
  local dbnode_host=${DBNODE_HOST:-dbnode01}
  local dbnode_port=${DBNODE_PORT:-9000}
  local dbnode_health_port=${DBNODE_HEALTH_PORT:-9002}
  local dbnode_id=${DBNODE_ID:-m3db_local}
  local coordinator_port=${COORDINATOR_PORT:-7201}
  local zone=${ZONE:-embedded}

  echo "Wait for API to be available"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace | jq ".namespaces | length")" == "0" ]'

  echo "Adding placement and agg namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/create -d '{
    "type": "cluster",
    "namespaceName": "agg",
    "retentionTime": "24h",
    "num_shards": 4,
    "replicationFactor": 1,
    "hosts": [
      {
          "id": "'${dbnode_id}'",
          "isolation_group": "rack-a",
          "zone": "'${zone}'",
          "weight": 1024,
          "address": "'"${dbnode_host}"'",
          "port": '"${dbnode_port}"'
      }
    ]
  }'

  echo "Updating aggregation options for agg namespace"
  curl -vvvsSf -X PUT 0.0.0.0:${coordinator_port}/api/v1/services/m3db/namespace -d '{
    "name": "agg",
    "options": {
      "aggregationOptions": {
        "aggregations": [
           {
            "aggregated": true,
            "attributes": {
              "resolutionDuration": "30s",
              "downsampleOptions": { "all": false }
            }
          }
        ]
      }
    }
  }'

  echo "Wait until placement is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/placement | jq .placement.instances.'${dbnode_id}'.id)" == \"'${dbnode_id}'\" ]'

  echo "Wait until agg namespace is ready"
  ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace/ready -d "{ \"name\": \"agg\"}" | grep -c true)" -eq 1 ]'

  wait_for_namespaces

  echo "Adding agg2d namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/namespace/create -d '{
    "namespaceName": "agg2d",
    "retentionTime": "48h"
  }'

  echo "Updating aggregation options for agg namespace"
  curl -vvvsSf -X PUT 0.0.0.0:${coordinator_port}/api/v1/services/m3db/namespace -d '{
    "name": "agg2d",
    "options": {
      "aggregationOptions": {
        "aggregations": [
          {
            "aggregated": true,
            "attributes": {
              "resolutionDuration": "1m",
              "downsampleOptions": { "all": false }
            }
          }
        ]
      }
    }
  }'

  echo "Wait until agg2d namespace is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace | jq .registry.namespaces.agg2d.indexOptions.enabled)" == true ]'

  echo "Wait until agg2d namespace is ready"
  ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace/ready -d "{ \"name\": \"agg2d\"}" | grep -c true)" -eq 1 ]'

  echo "Wait until bootstrapped"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_health_port}"'/health | jq .bootstrapped)" == true ]'
}

function setup_single_m3db_node {
  local dbnode_host=${DBNODE_HOST:-dbnode01}
  local dbnode_port=${DBNODE_PORT:-9000}
  local dbnode_health_port=${DBNODE_HEALTH_PORT:-9002}
  local dbnode_id=${DBNODE_ID:-m3db_local}
  local coordinator_port=${COORDINATOR_PORT:-7201}
  local zone=${ZONE:-embedded}
  local agg_resolution=${AGG_RESOLUTION:-15s}
  local agg_retention=${AGG_RETENTION:-10h}

  echo "Wait for API to be available"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace | jq ".namespaces | length")" == "0" ]'

  echo "Adding placement and agg namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/create -d '{
    "type": "cluster",
    "namespaceName": "agg",
    "retentionTime": "'${agg_retention}'",
    "num_shards": 4,
    "replicationFactor": 1,
    "hosts": [
      {
          "id": "'${dbnode_id}'",
          "isolation_group": "rack-a",
          "zone": "'${zone}'",
          "weight": 1024,
          "address": "'"${dbnode_host}"'",
          "port": '"${dbnode_port}"'
      }
    ]
  }'

  echo "Updating aggregation options for agg namespace"
  curl -vvvsSf -X PUT 0.0.0.0:${coordinator_port}/api/v1/services/m3db/namespace -d '{
    "name": "agg",
    "options": {
      "aggregationOptions": {
        "aggregations": [
          {
            "aggregated": true,
            "attributes": {
              "resolutionDuration": "'${agg_resolution}'"
            }
          }
        ]
      }
    }
  }'

  echo "Wait until placement is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/placement | jq .placement.instances.'${dbnode_id}'.id)" == \"'${dbnode_id}'\" ]'

  echo "Wait until agg namespace is ready"
  ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace/ready -d "{ \"name\": \"agg\"}" | grep -c true)" -eq 1 ]'

  wait_for_namespaces

  echo "Wait until bootstrapped"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_health_port}"'/health | jq .bootstrapped)" == true ]'
}

function setup_two_m3db_nodes {
  local dbnode_id_1=${DBNODE_ID_01:-m3db_local_1}
  local dbnode_id_2=${DBNODE_ID_02:-m3db_local_2}
  local dbnode_host_1=${DBNODE_HOST_01:-dbnode01}
  local dbnode_host_2=${DBNODE_HOST_02:-dbnode02}
  local dbnode_port=${DBNODE_PORT:-9000}
  local dbnode_host_1_health_port=${DBNODE_HEALTH_PORT_01:-9012}
  local dbnode_host_2_health_port=${DBNODE_HEALTH_PORT_02:-9022}
  local coordinator_port=${COORDINATOR_PORT:-7201}
  local agg_resolution=${AGG_RESOLUTION:-15s}
  local agg_retention=${AGG_RETENTION:-10h}

  echo "Wait for API to be available"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace | jq ".namespaces | length")" == "0" ]'

  echo "Adding placement and agg namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/create -d '{
    "type": "cluster",
    "namespaceName": "agg",
    "retentionTime": "'${agg_retention}'",
    "num_shards": 2,
    "replicationFactor": 2,
    "hosts": [
      {
          "id": "'"${dbnode_id_1}"'",
          "isolation_group": "rack-a",
          "zone": "embedded",
          "weight": 1024,
          "address": "'"${dbnode_host_1}"'",
          "port": '"${dbnode_port}"'
      },
      {
          "id": "'"${dbnode_id_2}"'",
          "isolation_group": "rack-b",
          "zone": "embedded",
          "weight": 1024,
          "address": "'"${dbnode_host_2}"'",
          "port": '"${dbnode_port}"'
      }
    ]
  }'

  echo "Updating aggregation options for agg namespace"
  curl -vvvsSf -X PUT 0.0.0.0:${coordinator_port}/api/v1/services/m3db/namespace -d '{
    "name": "agg",
    "options": {
      "aggregationOptions": {
        "aggregations": [
          {
            "aggregated": true,
            "attributes": {
              "resolutionDuration": "'${agg_resolution}'"
            }
          }
        ]
      }
    }
  }'

  echo "Wait until placement is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/placement | jq .placement.instances.'"${dbnode_id_1}"'.id)" == \"'"${dbnode_id_1}"'\" ]'

  echo "Wait until agg namespace is ready"
  ATTEMPTS=20 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace/ready -d "{ \"name\": \"agg\"}" | grep -c true)" -eq 1 ]'

  wait_for_namespaces

  echo "Wait until bootstrapped"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_host_1_health_port}"'/health | jq .bootstrapped)" == true ]'
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${dbnode_host_2_health_port}"'/health | jq .bootstrapped)" == true ]'
}

function wait_for_namespaces {
  local coordinator_port=${COORDINATOR_PORT:-7201}
  local unagg_retention=${UNAGG_RETENTION:-10h}

  echo "Wait until agg namespace is init'd"
  ATTEMPTS=10 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace | jq .registry.namespaces.agg.indexOptions.enabled)" == true ]'

  echo "Adding unagg namespace"
  curl -vvvsSf -X POST 0.0.0.0:${coordinator_port}/api/v1/database/namespace/create -d '{
    "namespaceName": "unagg",
    "retentionTime": "'${unagg_retention}'"
  }'

  echo "Wait until unagg namespace is init'd"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace | jq .registry.namespaces.unagg.indexOptions.enabled)" == true ]'

  echo "Wait until unagg namespace is ready"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace/ready -d "{ \"name\": \"unagg\"}" | grep -c true)" -eq 1 ]'

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
      },
      "aggregationOptions": {
        "aggregations": [
          { "aggregated": false }
        ]
      }
    }
  }'

  echo "Wait until coldWritesRepairAndNoIndex namespace is init'd"
  ATTEMPTS=100 MAX_TIMEOUT=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:'"${coordinator_port}"'/api/v1/services/m3db/namespace | jq .registry.namespaces.coldWritesRepairAndNoIndex.coldWritesEnabled)" == true ]'
}

