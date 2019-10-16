#!/usr/bin/env bash

set -ex
source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh

function write_metrics {
  NUM=$1
  echo "Writing $NUM metrics to [0.0.0.0:9003]"
  # set +x
  for (( i=0; i<$NUM; i++ ))
  do
    curl -X POST 0.0.0.0:9003/writetagged -d '{
      "namespace": "unagg",
      "id": "{__name__=\"'$METRIC_NAME'\",val=\"'$i'\"}",
      "tags": [
        {
          "name": "__name__",
          "value": "'$METRIC_NAME'"
        },
        { 
          "name": "val",
          "value": "'$i'"
        }
      ],
      "datapoint": {
        "timestamp":'"$t"',
        "value": 1
      }
    }'
  done
  # set -x
}
 
function test_instantaneous {
  QUERY=$1
  EXPECTED_COUNT=$2
  EXPECTED=$3
  RESPONSE=$(curl -sSL "http://localhost:7201/api/v1/query?query=$QUERY")
  echo $REPONSE | jq .data.result
  ACTUAL_COUNT=$(echo $RESPONSE | jq '.data.result | length')
  ACTUAL=$(echo $RESPONSE | jq .data.result[].metric.foo | tr -d "\n")
  CONCAT=$(echo $EXPECTED | tr -d " ")
  test $ACTUAL_COUNT = $EXPECTED_COUNT && test $ACTUAL = $CONCAT
}

function test_replace {
  export t=$(date +%s)
  METRIC_NAME="quail_$t"
  write_metrics 5
  sleep 1
  query='label_replace('$METRIC_NAME',"foo","bar_$1","val","(.*)")'
  test_instantaneous $query 5 "\"bar_0\" \"bar_1\" \"bar_2\" \"bar_3\" \"bar_4\""
  query='label_replace('$METRIC_NAME',"foo","bar_$1","val","(.*)")-0'
  test_instantaneous $query 5 "\"bar_0\" \"bar_1\" \"bar_2\" \"bar_3\" \"bar_4\""
}

function test_parse_query {
  test $(curl 'http://localhost:7201/api/v1/parse?query=up' | jq .name) = '"fetch"'
  THRESHOLD=$(curl 'http://localhost:7201/api/v1/threshold?query=up>1')
  test $(echo $THRESHOLD | jq .threshold.comparator) = '">"'
  test $(echo $THRESHOLD | jq .threshold.value) = 1
  test $(echo $THRESHOLD | jq .query.name) = '"fetch"'

  THRESHOLD=$(curl 'http://localhost:7201/api/v1/threshold?query=1>up')
  test $(echo $THRESHOLD | jq .threshold.comparator) = '"<"'
  test $(echo $THRESHOLD | jq .threshold.value) = 1
  test $(echo $THRESHOLD | jq .query.name) = '"fetch"'
}

function test_correctness {
  test_parse_query
  test_replace 
}
