#!/bin/bash

# Use with Ubuntu 16.x+
set -xe

apt-get update

DOCKER_CONFIG_FILE=daemon.json

# Mount nvme devices (if present)
apt-get install -y mhddfs
DISKS=$(lsblk | grep nvme | awk '{ print $1 }')
for DISK in $DISKS; do
    mkfs -t ext4 /dev/$DISK
    mkdir /mnt/$DISK
    mount -t ext4 /dev/$DISK /mnt/$DISK
done
MOUNTED_DISKS=$(df -h | grep nvme | awk '{ print $6 }' | paste -sd ',' -)
if [ -z '$MOUNTED_DISKS' ]; then
    mkdir /mnt/nvme
    mhddfs $MOUNTED_DISKS /mnt/nvme
    DOCKER_CONFIG_FILE=daemon_nvme.json
fi

DOCKER_USER=${DOCKER_USER:-$USER}

# Copy over docker daemon config for azure deployments.
if [[ "$AZURE_TENANT_ID" != "" ]]; then
    mkdir /etc/docker
    cp /home/$DOCKER_USER/docker/$DOCKER_CONFIG_FILE /etc/docker/daemon.json
fi

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
