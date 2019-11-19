#!/bin/bash 

# Use with Ubuntu 16.x+
set -xe

DOCKER_USER=${DOCKER_USER:-$USER}

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
