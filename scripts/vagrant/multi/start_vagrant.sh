#!/bin/bash

set -xe

export BOX="ubuntu/xenial64"
PROVIDER=${PROVIDER:-virtualbox}

if [[ "$PROVIDER" == "google" ]]; then
    export BOX="google/gce" 

    if [[ "$GOOGLE_PROJECT_ID" == "" ]]; then 
        echo "GOOGLE_PROJECT_ID env var not set"
        exit 1
    fi
    if [[ "$GOOGLE_JSON_KEY_LOCATION" == "" ]]; then 
        echo "GOOGLE_JSON_KEY_LOCATION env var not set"
        exit 1
    fi
    if [[ "$USER" == "" ]]; then 
        echo "USER env var not set"
        exit 1
    fi
    if [[ "$SSH_KEY" == "" ]]; then 
        echo "SSH_KEY env var not set"
        exit 1
    fi
fi

if [[ "$FEATURE_DOCKER_IMAGE" == "" ]]; then 
    echo "FEATURE_DOCKER_IMAGE env var not set"
    exit 1
fi

# Bring up boxes
echo "Provision boxes"
vagrant up --provider $PROVIDER

# Create ingress rules if not already exists
MAYBE_M3COORDINATOR_INGRESS=$(gcloud --project=studious-saga-237001 compute firewall-rules list 2> /dev/null | tail -n +2 | awk '{ print $1 };' | grep default-allow-m3coordinator) || true
if  [ "$MAYBE_M3COORDINATOR_INGRESS" != "default-allow-m3coordinator" ]; then
    gcloud --project=$GOOGLE_PROJECT_ID compute firewall-rules create default-allow-m3coordinator \
        --action=allow \
        --rules=tcp:7201 \
        --direction=ingress \
        --target-tags=network-m3coordinator
fi

# Provision clusters
echo "Provision k8s clusters"
vagrant ssh secondary -c "cd provision && MACHINE=secondary FEATURE_DOCKER_IMAGE=$FEATURE_DOCKER_IMAGE ./setup_kube.sh" &
vagrant ssh primary -c "cd provision && MACHINE=primary ./setup_kube.sh"

# Run tunnels forever
vagrant ssh secondary -c "cd provision && nohup ./run_tunnels.sh &"
vagrant ssh primary -c "cd provision && nohup ./run_tunnels.sh &"

# Run rolling restart forever
vagrant ssh secondary -c "cd provision && nohup ./rolling_restart_dbnodes.sh &"
vagrant ssh primary -c "cd provision && nohup ./rolling_restart_dbnodes.sh &"

# Setup benchmarker
# Get primary/secondary external IP addresses
M3COORDINATOR_PRIMARY_IP=$(gcloud --project=studious-saga-237001 compute instances list | grep primary | awk '{ print $5 }')
M3COORDINATOR_SECONDARY_IP=$(gcloud --project=studious-saga-237001 compute instances list | grep secondary | awk '{ print $5 }')
vagrant ssh benchmarker -c "cd provision && M3COORDINATOR_PRIMARY_IP=$M3COORDINATOR_PRIMARY_IP M3COORDINATOR_SECONDARY_IP=$M3COORDINATOR_SECONDARY_IP ./setup_kube_bench.sh"
