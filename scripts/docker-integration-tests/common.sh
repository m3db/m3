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

function wait_for_db_init {
  echo "Sleeping for a bit to ensure db up"
  sleep 15 # TODO Replace sleeps with logic to determine when to proceed

  echo "Adding namespace"
  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/namespace -d '{
    "name": "agg",
    "options": {
      "bootstrapEnabled": true,
      "flushEnabled": true,
      "writesToCommitLog": true,
      "cleanupEnabled": true,
      "snapshotEnabled": true,
      "repairEnabled": false,
      "retentionOptions": {
        "retentionPeriodNanos": 172800000000000,
        "blockSizeNanos": 7200000000000,
        "bufferFutureNanos": 600000000000,
        "bufferPastNanos": 600000000000,
        "blockDataExpiry": true,
        "blockDataExpiryAfterNotAccessPeriodNanos": 300000000000
      },
      "indexOptions": {
        "enabled": true,
        "blockSizeNanos": 7200000000000
      }
    }
  }'

  echo "Sleep until namespace is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.agg.indexOptions.enabled)" == true ]'

  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/namespace -d '{
    "name": "unagg",
    "options": {
      "bootstrapEnabled": true,
      "flushEnabled": true,
      "writesToCommitLog": true,
      "cleanupEnabled": true,
      "snapshotEnabled": true,
      "repairEnabled": false,
      "retentionOptions": {
        "retentionPeriodNanos": 172800000000000,
        "blockSizeNanos": 7200000000000,
        "bufferFutureNanos": 600000000000,
        "bufferPastNanos": 600000000000,
        "blockDataExpiry": true,
        "blockDataExpiryAfterNotAccessPeriodNanos": 300000000000
      },
      "indexOptions": {
        "enabled": true,
        "blockSizeNanos": 7200000000000
      }
    }
  }'

  echo "Sleep until namespace is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.unagg.indexOptions.enabled)" == true ]'

  echo "Placement initialization"
  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/placement/init -d '{
      "num_shards": 64,
      "replication_factor": 1,
      "instances": [
          {
              "id": "m3db_local",
              "isolation_group": "rack-a",
              "zone": "embedded",
              "weight": 1024,
              "endpoint": "dbnode01:9000",
              "hostname": "dbnode01",
              "port": 9000
          }
      ]
  }'

  echo "Sleep until placement is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | jq .placement.instances.m3db_local.id)" == \"m3db_local\" ]'

  echo "Sleep until bootstrapped"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:9002/health | jq .bootstrapped)" == true ]'

  echo "Waiting until shards are marked as available"
  ATTEMPTS=10 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | grep -c INITIALIZING)" -eq 0 ]'
}

