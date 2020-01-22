#!/bin/bash

set -xe

if [[ "$GRAFANA_PORT" == "" ]]; then
    GRAFANA_PORT="3333"
fi

if [[ "$USER" == "" ]]; then
    echo "USER env var not set"
    exit 1
fi
if [[ "$SSH_KEY" == "" ]]; then
    echo "SSH_KEY env var not set"
    exit 1
fi
if [[ "$IP_ADDRESS" == "" ]]; then
    echo "IP_ADDRESS env var not set"
    exit 1
fi

# Run tunnels
echo "Tunnelling"
echo "Grafana available at http://localhost:$GRAFANA_PORT"
ssh -N -i $SSH_KEY $USER@$IP_ADDRESS \
    -L $GRAFANA_PORT:localhost:3000 \
    -L 7201:localhost:7201
