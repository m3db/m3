#!/bin/bash

set -xe

export KUBECONFIG="$(kind get kubeconfig-path --name="kind")"

# Perform rolling restarts of db nodes forever.
NODE_NUM=0
while true
do
    if (( NODE_NUM > 2 )); then
        NODE_NUM=0
    fi

    $(kubectl exec test-cluster-rep$NODE_NUM-0 kill 1) || true
    # Wait until the killed node is bootstrapped before moving on to kill the next node.
    bootstrapped=false
    while [ "$bootstrapped" == "false" ]
    do
        if kubectl exec test-cluster-rep$NODE_NUM-0 -- curl -f localhost:9002/bootstrapped; then
            bootstrapped=true
        fi
        sleep 30s
    done

    NODE_NUM=$(( NODE_NUM + 1 ))
done
