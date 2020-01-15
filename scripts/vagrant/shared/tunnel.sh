#!/bin/bash

set -xe

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
echo "Grafana available at http://localhost:3333"
ssh -i $SSH_KEY $USER@$IP_ADDRESS "cd provision && ./run_tunnels.sh" &
ssh -N -i $SSH_KEY $USER@$IP_ADDRESS \
    -L 3333:localhost:3000 \
    -L 7201:localhost:7201 \
    -L 9003:localhost:9003 \
    -L 9004:localhost:9004 \
