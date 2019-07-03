#!/bin/bash 

set -xe

# Expects the following installed:
# - kubectl
# - kind
# - curl
# - jq

# Create cluster
kind create cluster

# Use correct kubeconfig
export KUBECONFIG="$(kind get kubeconfig-path --name="kind")"

# Apply common kube manifests
kubectl apply -f ./kube/sysctl-daemonset.yaml

# Create dedicated m3coordinator 2x
kubectl apply -f ./manifests/m3coordinator-two.yaml

# Deploy single node etcd
kubectl apply -f ./manifests/etcd-single.yaml

# Deploy operator
kubectl apply -f ./manifests/operator.yaml

# Create test cluster
kubectl apply -f ./manifests/m3db-single.yaml

# Comment out the set +x and consequent set -x to debug the wait script
set +x
echo "Waiting for cluster to come up"
while true; do
    if kubectl exec -it test-cluster-rep0-0 -- sh -c "(which curl || apk add curl) && curl http://localhost:9002/bootstrapped"; then
        printf "\n"
        break
    fi
    sleep 1
    printf "."
done
set -x

# Deploy monitoring with Prometheus
kubectl apply -f ./manifests/prometheus-operator.yaml
kubectl apply -f ./manifests/kube-prometheus-manifests

# Ready load generator (but don't scale up yet)
kubectl apply -f ./manifests/promremotebench-zero.yaml
