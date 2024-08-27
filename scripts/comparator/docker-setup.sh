#!/usr/bin/env bash

set -xe

REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/comparator/docker-compose.yml
export REVISION

function setup_docker {
  echo "Run m3query, m3comparator, and prometheus containers"
  docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes m3comparator
  pwd
  docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes prometheus
  docker-compose -f ${COMPOSE_FILE} logs prometheus
  docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes m3query
  docker-compose -f ${COMPOSE_FILE} logs m3query
  docker-compose -f ${COMPOSE_FILE} ps
  
  CI=$1
  if [[ "$CI" != "true" ]]
  then
    echo "run grafana container"
    docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes grafana
  fi
}

function teardown_docker {
  CI=$1
   # CI fails to stop all containers sometimes
  if [[ "$CI" == "true" ]]
  then
    docker-compose -f ${COMPOSE_FILE} down || echo "unable to shutdown containers"
  fi
}
