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
  echo "Wait for API to be available"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq ".namespaces | length")" == "0" ]'

  echo "Adding placement and agg namespace"
  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/database/create -d '{
    "type": "cluster",
    "namespaceName": "agg",
    "retentionTime": "24h",
    "replicationFactor": 1,
    "hosts": [
      {
          "id": "m3db_local",
          "isolation_group": "rack-a",
          "zone": "embedded",
          "weight": 1024,
          "address": "dbnode01",
          "port": 9000
      }
    ]
  }'

  echo "Wait until placement is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/placement | jq .placement.instances.m3db_local.id)" == \"m3db_local\" ]'

  echo "Wait until agg namespace is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.agg.indexOptions.enabled)" == true ]'

  echo "Adding unagg namespace"
  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/database/namespace/create -d '{
    "namespaceName": "unagg",
    "retentionTime": "24h"
  }'

  echo "Wait until unagg namespace is init'd"
  ATTEMPTS=4 TIMEOUT=1 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:7201/api/v1/namespace | jq .registry.namespaces.unagg.indexOptions.enabled)" == true ]'

  echo "Wait until bootstrapped"
  ATTEMPTS=10 TIMEOUT=2 retry_with_backoff  \
    '[ "$(curl -sSf 0.0.0.0:9002/health | jq .bootstrapped)" == true ]'
}

