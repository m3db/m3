#!/bin/bash

# Use with Ubuntu 16.x+
set -xe

DOCKER_USER=${DOCKER_USER:-$USER}

# Copy over docker daemon config for azure deployments.
if [[ "$AZURE_TENANT_ID" != "" ]]; then
    cp -r /home/$DOCKER_USER/docker /etc/docker
fi

apt-get update

# Install git
apt-get install -y git

# Install utilities
apt-get install -y tmux curl jq htop

# Install docker
apt-get install -y containerd docker.io
usermod -aG docker $DOCKER_USER

# Install kubectl
snap install kubectl --classic
ln -s /snap/bin/kubectl /usr/bin/kubectl
