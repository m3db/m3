#!/bin/bash

set -xe

# Use correct kubeconfig
export KUBECONFIG="$(kind get kubeconfig-path --name="kind")"

forwards="grafana:3000 m3coordinator:7201"

port_forward() {
    forward=$1
    app=$(echo $forward | cut -f 1 -d ":")
    port=$(echo $forward | cut -f 2 -d ":")
    pod=$(kubectl get pod -A -o jsonpath="{.items[?(@.metadata.labels.app == \"${app}\")].metadata.name}" | tr " " "\n" | head -n 1)
    namespace=$(kubectl get pod -A -o jsonpath="{.items[?(@.metadata.labels.app == \"${app}\")].metadata.namespace}" | tr " " "\n" | head -n 1)
    echo "port forwarding app $app port $port pod $pod namespace $namespace"
    kubectl port-forward -n $namespace $pod $port:$port
}

export -f port_forward

echo $forwards | tr " " "\n" | xargs -P 100 -I{} bash -c 'port_forward "{}"'
