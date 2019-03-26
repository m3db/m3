#!/bin/ash
# shellcheck shell=dash

# Checks for node health:
# 1. Is there a topology? If not, exit healthy.
# 2. Is the host in the topology? If not, exit healthy.
# 3. Is there a namespace? If not, exit healthy.
# 4. If 1-3 true, check if host reports bootstrapped and exit healthy if so
#    (fail otherwise).

set -o pipefail

COORDINATOR_PORT=${COORDINATOR_PORT:-7201}
DBNODE_PORT=${DBNODE_PORT:-9002}
COORD_PLACEMENT_ENDPOINT="http://localhost:${COORDINATOR_PORT}/api/v1/placement"
COORD_NAMESPACE_ENDPOINT="http://localhost:${COORDINATOR_PORT}/api/v1/namespace"
DBNODE_ENDPOINT="http://localhost:${DBNODE_PORT}/health"

HOSTNAME=${HOSTNAME:-$(hostname)}

COORD_TMPFILE=$(mktemp)
DBNODE_TMPFILE=$(mktemp)

cleanup() {
  rm -f "$COORD_TMPFILE" "$DBNODE_TMPFILE"
}

trap cleanup EXIT

curl --max-time 60 -sSf -o "$COORD_TMPFILE" "$COORD_PLACEMENT_ENDPOINT"
RES=$?

# Curl exits 22 for 400+ status code. Note this leaves us vulnerable to saying
# bootstrapped if our script makes a bad request and must use caution when
# modifying the script or the coordinator placement endpoint.
if [ "$RES" -eq 22 ]; then
  COORD_OUTPUT=$(cat "$COORD_TMPFILE")
  echo "Received 4xx from coordinator $COORD_PLACEMENT_ENDPOINT: $COORD_OUTPUT"
  exit 0
fi

# jq -e will exit 1 if the last value was null (or false)
jq -e ".placement.instances | .[\"${HOSTNAME}\"]" < "$COORD_TMPFILE" >/dev/null
RES=$?

if [ "$RES" -ne 0 ]; then
  echo "Host not present in topology"
  exit 0
fi

curl --max-time 60 -sSf -o "$COORD_TMPFILE" "$COORD_NAMESPACE_ENDPOINT"
RES=$?

if [ "$RES" -eq 22 ]; then
  echo "Received 4xx from namespace endpoint $COORD_NAMESPACE_ENDPOINT"
  exit 0
fi

# If there IS a placement but NO namespace, then the dbnode will respond
# connection refused (until https://github.com/m3db/m3/issues/996 is addressed)
# and the health script would fail hereafter. However, if there's no namespaces
# then we want to consider the node healthy because it just doesn't have
# anything to do. So before we ask the dbnode for its health, check if there's
# no namespaces to begin with, and if so exit 0.
NS_COUNT=$(jq '.registry.namespaces | length' < "$COORD_TMPFILE")
if [ "$NS_COUNT" -eq 0 ]; then
  echo "No namespaces in cluster"
  exit 0
fi

curl --max-time 60 -sSf -o "$DBNODE_TMPFILE" "$DBNODE_ENDPOINT"
RES=$?

if [ "$RES" -ne 0 ]; then
  echo "Received exit code $RES from curl health $DBNODE_ENDPOINT"
  exit 1
fi

BOOTSTRAPPED=$(jq .bootstrapped < "$DBNODE_TMPFILE")
if [ "$BOOTSTRAPPED" != "true" ]; then
  echo "Not bootstrapped ($BOOTSTRAPPED)"
  exit 1
fi

echo "Host bootstrapped"
exit 0
