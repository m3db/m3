#!/bin/bash 

set -xe

# Expects the following installed:
# - docker
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

# Populate the feature branch docker image
perl -pi -e "s/FEATURE_DOCKER_IMAGE/${FEATURE_DOCKER_IMAGE//\//\\/}/" ./manifests/m3db-secondary.yaml

# Create test cluster (sometimes the CRD not recognized, repeat attempts)
set +x
echo "Creating test cluster "
while true; do
    if kubectl apply -f ./manifests/m3db-$MACHINE.yaml; then
        printf "\n"
        break
    fi
    sleep 2
    printf "."
done
set -x

# Comment out the set +x and consequent set -x to debug the wait script
set +x
echo "Waiting for cluster to come up"
while true; do
    if kubectl exec -it test-cluster-rep0-0 -- sh -c "(which curl || apk add curl) && curl http://localhost:9002/bootstrapped"; then
        printf "\n"
        break
    fi
    sleep 2
    printf "."
done
set -x

# Deploy monitoring with Prometheus
# Promethues Operator
kubectl apply -f ./manifests/prometheus-operator.yaml 

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

# ServiceMonitor CRD for M3DB monitoring
kubectl apply -f ./manifests/prometheus-servicemonitor-dbnode.yaml
# ServiceMonitor CRD for M3Coordinator monitoring
kubectl apply -f ./manifests/prometheus-servicemonitor-coordinator.yaml

# Ready load generator (but don't scale up yet)
kubectl apply -f ./manifests/promremotebench-zero.yaml
