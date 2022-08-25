#!/usr/bin/env bash

set -xe

TEST_PATH="$M3_PATH"/scripts/docker-integration-tests
FANOUT_PATH=$TEST_PATH/query_fanout
source $TEST_PATH/common.sh
source $FANOUT_PATH/warning.sh
source $FANOUT_PATH/restrict.sh

REVISION=$(git rev-parse HEAD)
COMPOSE_FILE="$M3_PATH"/scripts/docker-integration-tests/query_fanout/docker-compose.yml
export REVISION

echo "Run m3dbnode and m3coordinator containers"
docker-compose-with-defaults -f ${COMPOSE_FILE} up -d dbnode-cluster-a
docker-compose-with-defaults -f ${COMPOSE_FILE} up -d coordinator-cluster-a

docker-compose-with-defaults -f ${COMPOSE_FILE} up -d dbnode-cluster-b
docker-compose-with-defaults -f ${COMPOSE_FILE} up -d coordinator-cluster-b

docker-compose-with-defaults -f ${COMPOSE_FILE} up -d dbnode-cluster-c
docker-compose-with-defaults -f ${COMPOSE_FILE} up -d coordinator-cluster-c

# think of this as a defer func() in golang
function defer {
  docker-compose-with-defaults -f ${COMPOSE_FILE} down || echo "unable to shutdown containers" # CI fails to stop all containers sometimes
}
trap defer EXIT

AGG_RESOLUTION=5s DBNODE_HOST=dbnode-cluster-a DBDNODE_PORT=9000 DBNODE_HEALTH_PORT=9002 COORDINATOR_PORT=7201 \
 setup_single_m3db_node

AGG_RESOLUTION=5s DBNODE_HOST=dbnode-cluster-b DBDNODE_PORT=19000 DBNODE_HEALTH_PORT=19002 COORDINATOR_PORT=17201 \
 setup_single_m3db_node

AGG_RESOLUTION=5s DBNODE_HOST=dbnode-cluster-c DBDNODE_PORT=29000 DBNODE_HEALTH_PORT=29002 COORDINATOR_PORT=27201 \
 setup_single_m3db_node

echo "Write data to cluster a"
curl -vvvsS -X POST 0.0.0.0:9003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-a\",endpoint=\"/request\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-a"
    },
    {
      "name": "endpoint",
      "value": "/request"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

echo "Write data to cluster b"
curl -vvvsS -X POST 0.0.0.0:19003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-b\",endpoint=\"/request\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-b"
    },
    {
      "name": "endpoint",
      "value": "/request"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

echo "Write data to cluster c"
curl -vvvsS -X POST 0.0.0.0:29003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-c\",endpoint=\"/request\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-c"
    },
    {
      "name": "endpoint",
      "value": "/request"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

function read {
  RESPONSE=$(curl "http://0.0.0.0:7201/api/v1/query?query=test_metric")
  ACTUAL=$(echo $RESPONSE | jq .data.result[].metric.cluster | sort)
  test "$(echo $ACTUAL)" = '"cluster-a" "cluster-b" "cluster-c"'
}

ATTEMPTS=5 TIMEOUT=1 retry_with_backoff read

function read_sum {
  RESPONSE=$(curl "http://0.0.0.0:7201/api/v1/query?query=sum(test_metric)")
  ACTUAL=$(echo $RESPONSE | jq .data.result[].value[1])
  test $ACTUAL = '"126.370370367"'
}

ATTEMPTS=5 TIMEOUT=1 retry_with_backoff read_sum

echo "Write local tagged data to cluster a"
curl -vvvsS -X POST 0.0.0.0:19003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-b\",endpoint=\"/request\",local-only=\"local\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-b"
    },
    {
      "name": "endpoint",
      "value": "/request"
    },
    {
      "name": "local-only",
      "value": "local"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

echo "Write remote tagged data to cluster b"
curl -vvvsS -X POST 0.0.0.0:19003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-b\",endpoint=\"/request\",remote-only=\"remote\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-b"
    },
    {
      "name": "endpoint",
      "value": "/request"
    },
    {
      "name": "remote-only",
      "value": "remote"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

echo "Write remote tagged data to cluster c"
curl -vvvsS -X POST 0.0.0.0:29003/writetagged -d '{
  "namespace": "unagg",
  "id": "{__name__=\"test_metric\",cluster=\"cluster-c\",endpoint=\"/request\",third-cluster=\"third\"}",
  "tags": [
    {
      "name": "__name__",
      "value": "test_metric"
    },
    {
      "name": "cluster",
      "value": "cluster-c"
    },
    {
      "name": "endpoint",
      "value": "/request"
    },
    {
      "name": "third-cluster",
      "value": "third"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'

function complete_tags {
  RESPONSE=$(curl "http://0.0.0.0:7201/api/v1/labels")
  ACTUAL=$(echo $RESPONSE | jq .data[])
  test "$(echo $ACTUAL)" = '"__name__" "cluster" "endpoint" "local-only" "remote-only" "third-cluster"'
}

ATTEMPTS=5 TIMEOUT=1 retry_with_backoff complete_tags

echo "running fanout warning tests"
test_fanout_warnings

echo "running restrict tests"
test_restrictions
