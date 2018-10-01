#!/bin/bash

# Exits with code 0 if either:
# - There is no placement
# - The current host is not in the placement
# - The current is in the placement AND all it's shards are AVAILABLE
#
# Exits 1 otherwise.

set -exo pipefail

COORDINATOR_PORT=${COORDINATOR_PORT:-7201}
ENDPOINT="http://localhost:${COORDINATOR_PORT}/api/v1/placement"
HOSTNAME=${HOSTNAME:-$(hostname)}

TMPFILE=$(mktemp)

function cleanup() {
  rm -f "$TMPFILE"
}

trap cleanup EXIT

curl -sSf -o "$TMPFILE" "$ENDPOINT"
RES=$?

# Curl exits 22 for 400+ status code. Note this leaves us vulnerable to saying
# bootstrapped if our script makes a bad request and must use caution when
# modifying the script or the coordinator placement endpoint.
if [[ "$RES" -eq 22 ]]; then
  echo "Received 400"
  exit 0
fi

# TODO(schallert): should a leaving node be considered bootstrapped?
TOTAL=$(jq ".placement.instances | .[\"${HOSTNAME}\"] | .shards | length" < "$TMPFILE")

if [[ "$TOTAL" -eq 0 ]]; then
  # Host is not in placement.
  echo "$HOSTNAME not found in placement"
  exit 0
fi

AVAILABLE=$(jq ".placement.instances | .[\"${HOSTNAME}\"] | .shards | map(select(.state == \"AVAILABLE\")) | length" < "$TMPFILE")

if [[ "$AVAILABLE" -ne "$TOTAL" ]]; then
  echo "Available shards not equal total ($AVAILABLE != $TOTAL)"
  exit 1
fi

exit 0
