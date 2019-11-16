#!/bin/bash 

set -xe

# Expects the following installed:
# - docker
# - kubectl
# - kind
# - curl
# - jq

if [[ "$M3COORDINATOR_PRIMARY_IP" == "" ]]; then 
    echo "M3COORDINATOR_PRIMARY_IP env var not set"
    exit 1
fi
if [[ "$M3COORDINATOR_SECONDARY_IP" == "" ]]; then 
    echo "M3COORDINATOR_SECONDARY_IP env var not set"
    exit 1
fi

# Create cluster
kind create cluster

# Use correct kubeconfig
export KUBECONFIG="$(kind get kubeconfig-path --name="kind")"

# Populate m3ccord primary/secondary IPs
M3COORDINATOR_PRIMARY_WRITE_ADDR="http:\/\/$M3COORDINATOR_PRIMARY_IP:7201\/api\/v1\/prom\/remote\/write"
M3COORDINATOR_SECONDARY_WRITE_ADDR="http:\/\/$M3COORDINATOR_SECONDARY_IP:7201\/api\/v1\/prom\/remote\/write"
perl -pi -e "s/M3COORDINATOR_WRITE_TARGETS/$M3COORDINATOR_PRIMARY_WRITE_ADDR,$M3COORDINATOR_SECONDARY_WRITE_ADDR/" ./manifests/promremotebench-multi.yaml

M3COORDINATOR_PRIMARY_QUERY_ADDR="http:\/\/$M3COORDINATOR_PRIMARY_IP:7201\/api\/v1\/query_range"
M3COORDINATOR_SECONDARY_QUERY_ADDR="http:\/\/$M3COORDINATOR_SECONDARY_IP:7201\/api\/v1\/query_range"
perl -pi -e "s/M3COORDINATOR_QUERY_TARGETS/$M3COORDINATOR_PRIMARY_QUERY_ADDR,$M3COORDINATOR_SECONDARY_QUERY_ADDR/" ./manifests/promremotebench-multi.yaml

# Ready load generator (but don't scale up yet)
kubectl apply -f ./manifests/promremotebench-multi.yaml
