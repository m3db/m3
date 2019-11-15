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
perl -pi -e "s/M3COORDINATOR_PRIMARY_IP/$M3COORDINATOR_PRIMARY_IP/" ./manifests/promremotebench-multi.yaml
perl -pi -e "s/M3COORDINATOR_SECONDARY_IP/$M3COORDINATOR_SECONDARY_IP/" ./manifests/promremotebench-multi.yaml

# Ready load generator (but don't scale up yet)
kubectl apply -f ./manifests/promremotebench-multi.yaml
