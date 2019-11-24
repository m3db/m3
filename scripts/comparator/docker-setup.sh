#!/usr/bin/env bash

set -xe

REVISION=$(git rev-parse HEAD)
COMPOSE_FILE=$GOPATH/src/github.com/m3db/m3/scripts/comparator/docker-compose.yml
export REVISION

function setup_docker {
  echo "Run m3query, m3comparator, and prometheus containers"
  docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes m3comparator
  docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes prometheus
  docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes m3query
  docker-compose -f ${COMPOSE_FILE} up -d --build --renew-anon-volumes jaeger
  sleep 3
  # rely on 204 status code until https://github.com/jaegertracing/jaeger/issues/1450 is resolved.
  JAEGER_STATUS=$(curl -s -o /dev/null -w '%{http_code}' localhost:14269)
  if [ $JAEGER_STATUS -ne 204 ]; then
      echo "Jaeger could not start"
      return 1
  fi

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
