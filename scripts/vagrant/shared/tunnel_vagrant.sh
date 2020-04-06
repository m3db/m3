#!/bin/bash

set -xe

export BOX="ubuntu/xenial64"
PROVIDER=${PROVIDER:-virtualbox}

if [[ "$GRAFANA_PORT" == "" ]]; then
    GRAFANA_PORT="3333"
fi

if [[ "$PROVIDER" != "virtualbox" ]]; then
    if [[ "$USER" == "" ]]; then
        echo "USER env var not set"
        exit 1
    fi
    if [[ "$SSH_KEY" == "" ]]; then
        echo "SSH_KEY env var not set"
        exit 1
    fi
fi

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

if [[ "$PROVIDER" == "azure" ]]; then
    export BOX="azure"

    if [[ "$AZURE_TENANT_ID" == "" ]]; then
        echo "AZURE_TENANT_ID env var not set"
        exit 1
    fi
    if [[ "$AZURE_CLIENT_ID" == "" ]]; then
        echo "AZURE_CLIENT_ID env var not set"
        exit 1
    fi
    if [[ "$AZURE_CLIENT_SECRET" == "" ]]; then
        echo "AZURE_CLIENT_SECRET env var not set"
        exit 1
    fi
    if [[ "$AZURE_SUBSCRIPTION_ID" == "" ]]; then
        echo "AZURE_SUBSCRIPTION_ID env var not set"
        exit 1
    fi
fi

# Run tunnels
echo "Tunnelling"
echo "Grafana available at http://localhost:$GRAFANA_PORT"
vagrant ssh $MACHINE --no-tty --\
    -L $GRAFANA_PORT:localhost:3000 \
    -L 7201:localhost:7201
