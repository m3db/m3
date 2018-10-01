#!/bin/bash

# Exits with code 0 if either:
# - There is no placement
# - The current host reports that it is bootstrapped.
#
# Exits 1 otherwise.

set -exo pipefail

COORDINATOR_PORT=${COORDINATOR_PORT:-7201}
DBNODE_PORT=${DBNODE_PORT:-9004}
COORD_ENDPOINT="http://localhost:${COORDINATOR_PORT}/api/v1/placement"
DBNODE_ENDPOINT="http://localhost:${DBNODE_PORT}:9004/"

HOSTNAME=${HOSTNAME:-$(hostname)}

COORD_TMPFILE=$(mktemp)
DBNODE_TMPFILE=$(mktemp)

function cleanup() {
  rm -f "$COORD_TMPFILE" "$DBNODE_TMPFILE"
}

trap cleanup EXIT

curl -sSf -o "$COORD_TMPFILE" "$COORD_ENDPOINT"
RES=$?

# Curl exits 22 for 400+ status code. Note this leaves us vulnerable to saying
# bootstrapped if our script makes a bad request and must use caution when
# modifying the script or the coordinator placement endpoint.
if [[ "$RES" -eq 22 ]]; then
  echo "Received 4xx from coordinator"
  exit 0
fi

curl -sSf -o "$DBNODE_TMPFILE" "$DBNODE_ENDPOINT"
BOOTSTRAPPED=$(jq .bootstrapped < "$DBNODE_TMPFILE")
if [[ "$BOOTSTRAPPED" != "true" ]]; then
  echo "Not bootstrapped ($BOOTSTRAPPED)"
  exit 1
fi

exit 0
