#!/usr/bin/env bash

set -xe

REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/comparator/docker-compose.yml
export REVISION

echo "Run m3query, m3comparator, and prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d m3query
docker-compose -f ${COMPOSE_FILE} up -d m3comparator
docker-compose -f ${COMPOSE_FILE} up -d prometheus
