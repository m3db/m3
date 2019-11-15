#!/bin/bash

set -xe

# Perform rollign restarts of db nodes forever.
NODE_NUM=0
while true
do
    if (( NODE_NUM > 2 )); then
        NODE_NUM=0
    fi

    kubectl exec test-cluster-rep$NODE_NUM-0 kill 1
    sleep 5m

    NODE_NUM=$(( NODE_NUM + 1 ))
done
