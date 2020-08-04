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
kind create cluster  --config ./manifests/kind-kube-cluster.yaml

# Use correct kubeconfig
export KUBECONFIG="$(kind get kubeconfig-path --name="kind")"

# Manifests for Operator (prom, grafana, etc)
set +x
echo "Applying Prometheus operator manifests"
while true; do
    if kubectl apply -f ./manifests/kube-prometheus; then
        printf "\n"
        break
    fi
    sleep 2
    printf "."
done
set -x

# ServiceMonitor CRD for Promremotebench monitoring
kubectl apply -f ./manifests/prometheus-servicemonitor-promremotebench.yaml

# Populate m3ccord primary/secondary IPs
M3COORDINATOR_PRIMARY_WRITE_ADDR="http:\/\/$M3COORDINATOR_PRIMARY_IP:7201\/api\/v1\/prom\/remote\/write"
M3COORDINATOR_SECONDARY_WRITE_ADDR="http:\/\/$M3COORDINATOR_SECONDARY_IP:7201\/api\/v1\/prom\/remote\/write"
perl -pi -e "s/M3COORDINATOR_WRITE_TARGETS/$M3COORDINATOR_PRIMARY_WRITE_ADDR,$M3COORDINATOR_SECONDARY_WRITE_ADDR/" ./manifests/promremotebench-multi.yaml

M3COORDINATOR_PRIMARY_QUERY_ADDR="http:\/\/$M3COORDINATOR_PRIMARY_IP:7201\/api\/v1\/query_range"
M3COORDINATOR_SECONDARY_QUERY_ADDR="http:\/\/$M3COORDINATOR_SECONDARY_IP:7201\/api\/v1\/query_range"
perl -pi -e "s/M3COORDINATOR_QUERY_TARGETS/$M3COORDINATOR_PRIMARY_QUERY_ADDR,$M3COORDINATOR_SECONDARY_QUERY_ADDR/" ./manifests/promremotebench-multi.yaml

# Ready load generator (but don't scale up yet)
kubectl apply -f ./manifests/promremotebench-multi.yaml
