#!/usr/bin/env bash

set -xe

source "$M3_PATH"/scripts/docker-integration-tests/common.sh
REVISION=$(git rev-parse HEAD)
SCRIPT_PATH="$M3_PATH"/scripts/docker-integration-tests/dedicated_etcd_embedded_coordinator
COMPOSE_FILE=$SCRIPT_PATH/docker-compose.yml
export REVISION

echo "Run etcd and m3dbnode containers"
docker-compose-with-defaults -f ${COMPOSE_FILE} up -d --renew-anon-volumes etcd01
docker-compose-with-defaults -f ${COMPOSE_FILE} up -d --renew-anon-volumes dbnode01

DUMP_DIR="${SCRIPT_PATH}/dump"
DUMP_ZIP="${DUMP_DIR}/dump.zip"

function defer {
  if [ -d $DUMP_DIR ]; then 
    rm -rf $DUMP_DIR
  fi
  docker-compose-with-defaults -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

# Should be able to setup single db node with custom environment and zone
# using the embedded coordinator without special headers
DBNODE_ID="dbnode01" ZONE="bar-zone" setup_single_m3db_node

echo "Test the debug dump endpoint works with custom env and zone"
mkdir -p $DUMP_DIR
curl -s http://localhost:9004/debug/dump > $DUMP_ZIP

unzip -d $DUMP_DIR $DUMP_ZIP

EXPECTED_FILES="cpu.prof heap.prof goroutine.prof host.json namespace.json placement-m3db.json"
for file in $(echo "${EXPECTED_FILES}" | tr " " "\n"); do
  if ! [ -f "${DUMP_DIR}/${file}" ]; then
    echo "Expected ${file} but not in dump:"
    echo $(ls $DUMP_DIR)
    exit 1
  fi
done
