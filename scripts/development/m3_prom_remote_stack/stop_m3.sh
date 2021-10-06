#!/usr/bin/env bash

set -xe

if [[ "$WIPE_DATA" == true ]]; then
    docker-compose -f docker-compose.yml down
else
    docker-compose -f docker-compose.yml stop
fi
