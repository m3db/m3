#!/usr/bin/env bash

set -xe

echo "Stopping M3 Consumer Service..."

# Stop and remove the consumer
docker-compose down

echo "Consumer service stopped!"