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

# Bring up box
echo "Provision box"
vagrant up --provider $PROVIDER

# Provision cluster. Single machine ONLY operates on primary.
echo "Provision k8s cluster"
vagrant ssh -c 'MACHINE=primary cd provision && ./setup_kube.sh'

# Run tunnels
echo "Tunnelling"
echo "Grafana available at http://localhost:3333"
vagrant ssh -c "cd provision && ./run_tunnels.sh" --\
    -L 3333:localhost:3000 \
    -L 7201:localhost:7201 \
    -L 9003:localhost:9003 \
    -L 9004:localhost:9004 \
