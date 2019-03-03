#!/bin/bash

# The script makes it easier to run m3nsch by gathering the endpoints of all
# agents and constructing the base m3nsch_client command.
#
# Example usage:
# ./m3nsch_client.sh init -t foo -z default_zone -v default_env -n metrics-10s:2d -c 50000 -i 5000 -u 0.5
# ./m3nsch_client.sh start
# ./m3nsch_client.sh stop

# Get endpoints of the agents
function get_endpoints() {
  local jsonpath='{range .items[*]}{.status.podIP}:{.spec.containers[0].ports[0].containerPort},{end}'
  # cut trailing comma
  kubectl get po -l app=m3nsch,component=agent -o jsonpath="$jsonpath" | sed 's/,$//'
}

CLIENT_POD=$(kubectl get po | grep client | awk '{print $1}')

if [[ -z "$CLIENT_POD" ]]; then
  echo "could not find client pod"
  exit 1
fi

AGENT_ENDPOINTS=$(get_endpoints)

set -x
kubectl exec "$CLIENT_POD" -- ./bin/m3nsch_client -e "$AGENT_ENDPOINTS" "$@"
set +x
