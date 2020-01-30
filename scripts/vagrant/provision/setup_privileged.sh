#!/bin/bash

# Use with Ubuntu 16.x+
set -xe

apt-get update

DOCKER_CONFIG_FILE=daemon.json

# Mount nvme devices (if present)
DISKS=$(lsblk | grep nvme | awk '{ print "/dev/"$1 }')
if [[ "$DISKS" != "" ]]; then
    VG=nvme_vg
    LV=nvme_lv
    vgcreate $VG $DISKS
    # This gets the size in GB.
    SIZE_GB=$(vgs | grep $VG | awk '{ print $6 }' | tr -dc 0-9 | xargs expr 10 "*")
    lvcreate -L "$SIZE_GB"G --name $LV $VG
    mkfs -t ext4 /dev/$VG/$LV
    mkdir -p /mnt/nvme
    mount /dev/$VG/$LV /mnt/nvme
    DOCKER_CONFIG_FILE=daemon_nvme.json
fi

DOCKER_USER=${DOCKER_USER:-$USER}

# Copy over docker daemon config
mkdir /etc/docker
cp /home/$DOCKER_USER/docker/$DOCKER_CONFIG_FILE /etc/docker/daemon.json

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
