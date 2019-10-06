#!/usr/bin/env bash

set -ex
source $GOPATH/src/github.com/m3db/m3/scripts/docker-integration-tests/common.sh

function write_metrics {
  NUM=$1
  echo "Writing $NUM metrics to [0.0.0.0:9003]"
  set +x
  for (( i=0; i<$NUM; i++ ))
  do
    curl -X POST 0.0.0.0:$PORT/writetagged -d '{
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
  set -x
}
 
function test_instantaneous {
  QUERY=$1
  EXPECTED_COUNT=$2
  EXPECTED=$3
  RESPONSE=$(curl -sSL "http://localhost:7201/api/v1/query?query=$QUERY")
  ACTUAL_COUNT=$(echo $RESPONSE | jq '.data.result | length')
  ACTUAL=$(echo $RESPONSE | jq .data.result[].metric.foo | tr "\n" ":")
  CONCAT=$(echo $EXPECTED | tr " " ":")
  test $ACTUAL_COUNT = $EXPECTED_COUNT && test $ACTUAL = $CONCAT
}

function test_replace {
  METRIC_NAME="quail_$t"
  write_metrics coordinator-cluster-a 5
  test_instantaneous "label_replace($METRIC_NAME,\"foo\",\"bar_\$1\",\"val\",\"(.*)\")" label_replace 5 "\"bar_0\" \"bar_1\" \"bar_2\" \"bar_3\" \"bar_4\""
  test_instantaneous "label_replace($METRIC_NAME,\"foo\",\"bar_\$1\",\"val\",\"(.*)\")+0" label_replace 5 "\"bar_0\" \"bar_1\" \"bar_2\" \"bar_3\" \"bar_4\"" 
}

function test_correctness {
  test_replace 
}
