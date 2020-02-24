#!/bin/bash

set -xe

export BOX="ubuntu/xenial64"
PROVIDER=${PROVIDER:-virtualbox}

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

    # Create ingress rules if not already exists.
    MAYBE_M3COORDINATOR_INGRESS=$(gcloud --project=studious-saga-237001 compute firewall-rules list 2> /dev/null | tail -n +2 | awk '{ print $1 };' | grep default-allow-m3coordinator) || true
    if  [ "$MAYBE_M3COORDINATOR_INGRESS" != "default-allow-m3coordinator" ]; then
        gcloud --project=$GOOGLE_PROJECT_ID compute firewall-rules create default-allow-m3coordinator \
            --action=allow \
            --rules=tcp:7201 \
            --direction=ingress \
            --target-tags=network-m3coordinator
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

    # Group numbers
    if [[ "$GROUP0" == "" ]]; then
        export GROUP0="0"
    fi
    if [[ "$GROUP1" == "" ]]; then
        export GROUP1="1"
    fi
    if [[ "$GROUP2" == "" ]]; then
        export GROUP2="2"
    fi

    # Create resource groups if not already exists. There should be three (primary/secondary/benchmarker).
    function create_resource_group_if_not_exists() {
        RESOURCE_GROUP=$1
        MAYBE_RESOURCE_GROUP=$(az group list -o table 2> /dev/null | tail -n +3 | awk '{ print $1 };' | grep "\<$RESOURCE_GROUP\>") || true
        if  [[ "$MAYBE_RESOURCE_GROUP" != "$RESOURCE_GROUP" ]]; then
            az group create -n $RESOURCE_GROUP -l eastus
        fi
    }
    create_resource_group_if_not_exists vagrant-dev$GROUP0
    create_resource_group_if_not_exists vagrant-dev$GROUP1
    create_resource_group_if_not_exists vagrant-dev$GROUP2
fi

if [[ "$FEATURE_DOCKER_IMAGE" == "" ]]; then
    echo "FEATURE_DOCKER_IMAGE env var not set"
    exit 1
fi

# Bring up boxes
echo "Provision boxes"
vagrant up --provider $PROVIDER

# NB(bodu): We do this later because the network nsg gets automatically created by the vagrant plugin in the
# form of `${azure.nsg_name}-vagrantNSG`.
if [[ "$PROVIDER" == "azure" ]]; then
    function create_ingress_if_not_exists() {
        RESOURCE_GROUP=$1
        MAYBE_GROUP0_M3COORDINATOR_INGRESS=$(az network nsg rule list -g $RESOURCE_GROUP --nsg-name network-m3coordinator-vagrantNSG -o table 2> /dev/null | tail -n +3 | awk '{ print $1 };' | grep m3coordinator) || true
        if  [ "$MAYBE_GROUP0_M3COORDINATOR_INGRESS" != "default-allow-m3coordinator" ]; then
            az network nsg rule create -n default-allow-m3coordinator --nsg-name network-m3coordinator-vagrantNSG \
                --access Allow \
                --protocol Tcp \
                --direction Inbound \
                --destination-port-ranges 7201 \
                --priority 100 \
                -g $RESOURCE_GROUP
        fi
    }
    # Create m3coordinator ingress rules if not already exists.
    create_ingress_if_not_exists vagrant-dev$GROUP0
    create_ingress_if_not_exists vagrant-dev$GROUP1
fi

# Get primary/secondary external IP addresses
if [[ "$PROVIDER" == "google" ]]; then
    M3COORDINATOR_PRIMARY_IP=$(gcloud --project=studious-saga-237001 compute instances list | grep "\<primary-$USER$GROUP0\>" | awk '{ print $5 }')
    M3COORDINATOR_SECONDARY_IP=$(gcloud --project=studious-saga-237001 compute instances list | grep "\<secondary-$USER$GROUP1\>" | awk '{ print $5 }')
fi
if [[ "$PROVIDER" == "azure" ]]; then
    M3COORDINATOR_PRIMARY_IP=$(az vm list-ip-addresses -o table | grep primary-$USER$GROUP0 | awk '{ print $2 }')
    M3COORDINATOR_SECONDARY_IP=$(az vm list-ip-addresses -o table | grep secondary-$USER$GROUP1 | awk '{ print $2 }')
fi

# Provision clusters
echo "Provision k8s clusters"
vagrant ssh benchmarker -c "cd provision && M3COORDINATOR_PRIMARY_IP=$M3COORDINATOR_PRIMARY_IP M3COORDINATOR_SECONDARY_IP=$M3COORDINATOR_SECONDARY_IP ./setup_kube_bench.sh" &
vagrant ssh secondary -c "cd provision && MACHINE=secondary FEATURE_DOCKER_IMAGE=$FEATURE_DOCKER_IMAGE ./setup_kube.sh" &
vagrant ssh primary -c "cd provision && MACHINE=primary ./setup_kube.sh"

# Run rolling restart forever
vagrant ssh secondary -c "cd provision && nohup ./rolling_restart_dbnodes.sh & sleep 1"
vagrant ssh primary -c "cd provision && nohup ./rolling_restart_dbnodes.sh & sleep 1"
