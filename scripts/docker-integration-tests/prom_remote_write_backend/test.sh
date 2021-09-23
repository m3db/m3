#!/usr/bin/env bash

set -xe

M3_PATH=${M3_PATH:-$GOPATH/src/github.com/m3db/m3}
TESTDIR="$M3_PATH"/scripts/docker-integration-tests/
REVISION=$(git rev-parse HEAD)
export REVISION
COMPOSE_FILE="$TESTDIR"/prom_remote_write_backend/docker-compose.yml
PROMREMOTECLI_IMAGE=quay.io/m3db/prometheus_remote_client_golang:v0.4.3
JQ_IMAGE=realguess/jq:1.4@sha256:300c5d9fb1d74154248d155ce182e207cf6630acccbaadd0168e18b15bfaa786
TEST_SUCCESS=false

source "$TESTDIR"/common.sh
source "$TESTDIR"/prom_remote_write_backend/utils.sh
source "$TESTDIR"/prom_remote_write_backend/tests.sh

echo "Pull containers required for test"
docker pull $PROMREMOTECLI_IMAGE
docker pull $JQ_IMAGE

trap 'cleanup ${COMPOSE_FILE} ${TEST_SUCCESS}' EXIT

echo "Run ETCD"
docker-compose -f "${COMPOSE_FILE}" up -d etcd01

echo "Run Coordinator in Admin mode"
docker-compose -f "${COMPOSE_FILE}" up -d coordinatoradmin
wait_until_ready "0.0.0.0:7201"

initialize_m3_via_coordinator_admin

echo "Run M3 containers"
docker-compose -f "${COMPOSE_FILE}" up -d m3aggregator01
docker-compose -f "${COMPOSE_FILE}" up -d m3aggregator02
docker-compose -f "${COMPOSE_FILE}" up -d m3coordinator01

echo "Start Prometheus containers"
docker-compose -f "${COMPOSE_FILE}" up -d prometheusraw
docker-compose -f "${COMPOSE_FILE}" up -d prometheusagg

wait_until_leader_elected
wait_until_ready "0.0.0.0:7202"

echo "Running tests"

test_prometheus_remote_write_multi_namespaces

TEST_SUCCESS=true
