#!/usr/bin/env bash

set -ex
set -o pipefail

COMPOSE_FILE=./scripts/dtest/docker-compose.yml

function defer {
  docker-compose -f "${COMPOSE_FILE}" down
}

trap defer EXIT

docker-compose -f "${COMPOSE_FILE}" up --detach

go test -v -tags=dtest ./src/cmd/tools/dtest/docker/harness
