#!/usr/bin/env bash

set -ex

TESTS=(
	scripts/docker-integration-tests/simple/test.sh
	scripts/docker-integration-tests/cold_writes_simple/test.sh
	scripts/docker-integration-tests/prometheus/test.sh
	scripts/docker-integration-tests/carbon/test.sh
	scripts/docker-integration-tests/aggregator/test.sh
	scripts/docker-integration-tests/query_fanout/test.sh
	scripts/docker-integration-tests/repair/test.sh
	scripts/docker-integration-tests/replication/test.sh
	scripts/docker-integration-tests/repair_and_replication/test.sh
)

scripts/docker-integration-tests/setup.sh

NUM_TESTS=${#TESTS[@]}
MIN_IDX=$((NUM_TESTS*BUILDKITE_PARALLEL_JOB/BUILDKITE_PARALLEL_JOB_COUNT))
MAX_IDX=$(((NUM_TESTS*(BUILDKITE_PARALLEL_JOB+1)/BUILDKITE_PARALLEL_JOB_COUNT)-1))

ITER=0
for test in "${TESTS[@]}"; do
	if [[ $ITER -ge $MIN_IDX && $ITER -le $MAX_IDX ]]; then
		# Ensure all docker containers have been stopped so we don't run into issues
		# trying to bind ports.
		docker rm -f $(docker ps -aq) 2>/dev/null || true
		echo "----------------------------------------------"
		echo "running $test"
		"$test"
	fi
	ITER="$((ITER+1))"
done
