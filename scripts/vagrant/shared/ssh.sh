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

ssh -i $SSH_KEY $USER@$IP_ADDRESS
