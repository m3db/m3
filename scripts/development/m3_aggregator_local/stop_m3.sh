#!/usr/bin/env bash

set -xe

echo "Stopping M3 Aggregator Local Setup..."

# Stop all services using the generated compose file if it exists
if [ -f docker-compose.yml.generated ]; then
    docker-compose -f docker-compose.yml.generated down --volumes
else
    echo "No generated docker-compose file found, trying to stop any running containers..."

    # Stop any containers that might be running
    containers=$(docker ps -q --filter "label=com.docker.compose.project=m3_aggregator_local" 2>/dev/null || true)
    if [ ! -z "$containers" ]; then
        docker stop $containers
        docker rm $containers
    fi
fi

# Clean up any generated configuration files
M3_AGGREGATOR_NODE_COUNT=${M3_AGGREGATOR_NODE_COUNT:-10} # clean up to 10 nodes max
for i in $(seq 1 $M3_AGGREGATOR_NODE_COUNT); do
    node_id=$(printf "%02d" $i)
    rm -f "m3aggregator${node_id}.yml"
done

rm -f docker-compose.yml.generated

echo "M3 Aggregator Local Setup stopped and cleaned up."
