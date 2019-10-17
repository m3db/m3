#!/usr/bin/env bash

set -xe

REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/comparator/docker-compose.yml
export REVISION
docker-compose -f ${COMPOSE_FILE} stop m3comparator
docker-compose -f ${COMPOSE_FILE} stop m3query

echo "Run m3query, m3comparator, and prometheus containers"
docker-compose -f ${COMPOSE_FILE} up -d m3comparator
# docker-compose -f ${COMPOSE_FILE} up -d --build grafana
# docker-compose -f ${COMPOSE_FILE} up -d --build prometheus
docker-compose -f ${COMPOSE_FILE} up -d m3query
