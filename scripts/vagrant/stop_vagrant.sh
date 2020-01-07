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
echo "Terminate box"
vagrant destroy --provider $PROVIDER
