#!/usr/bin/env bash

set -ex
M3_PATH=${M3_PATH:-$GOPATH/src/github.com/m3db/m3}
source "$M3_PATH"/scripts/docker-integration-tests/common.sh

COMPOSE_FILE="$M3_PATH"/scripts/docker-integration-tests/query_fanout/docker-compose.yml
HEADER_FILE=headers.out

function write_agg_metrics {
  NUM=$1
  echo "Writing $NUM metrics to [0.0.0.0:9003]"
  set +x
  for (( i=0; i<$NUM; i++ ))
  do
    curl -s -X POST 0.0.0.0:9003/writetagged -d '{
      "namespace": "unagg",
      "id": "{__name__=\"'$METRIC_NAME'\",'$METRIC_NAME'=\"'$i'\"}",
      "tags": [
        {
          "name": "__name__",
          "value": "'$METRIC_NAME'"
        },
        {
          "name": "'$METRIC_NAME'",
          "value": "'$i'"
        }
      ],
      "datapoint": {
        "timestamp":'"$NOW"',
        "value": '$i'
      }
    }' &> /dev/null
  done

  set -x
}

function test_correct_label_values {
  RESULT=$(curl "http://0.0.0.0:7201/api/v1/label/${METRIC_NAME}/values" )
  COUNT=$(echo $RESULT | jq .data[] | wc -l)
  test $COUNT = 60
}

function test_failing_label_values {
  RESULT=$(curl "http://0.0.0.0:7201/api/v1/label/${METRIC_NAME}/values" )
  STATUS=$(echo $RESULT | jq .status)
  test $STATUS = '"error"'
}

function test_query_succeeds {
  RESULT=$(curl "http://0.0.0.0:7201/api/v1/query?query=sum($METRIC_NAME)&start=$NOW")
  STATUS=$(echo $RESULT | jq .status)
  test $STATUS = '"success"'
}

function test_global_aggregate_limits {  
  export NOW=$(date +"%s")
  export METRIC_NAME="aggregate_limits_$NOW"
  
  write_agg_metrics 60
  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/kvstore -d '{
    "key": "m3db.query.limits",
    "value":{
      "maxRecentlyQueriedMetadataRead": {
        "limit":150,
        "lookbackSeconds":5,
        "forceExceeded":false
      }
    },
    "commit":true
  }'

  # Make sure any existing limit has expired before continuing.
  ATTEMPTS=5 retry_with_backoff test_correct_label_values
  ATTEMPTS=5 retry_with_backoff test_correct_label_values
  ATTEMPTS=5 TIMEOUT=1 retry_with_backoff test_failing_label_values
  # Make sure that a query is unaffected by the the metadata limits.
  ATTEMPTS=2 retry_with_backoff test_query_succeeds
  # Make sure the limit expires within 10 seconds and the query succeeds again.
  ATTEMPTS=10 retry_with_backoff test_correct_label_values
  curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/kvstore -d '{
    "key": "m3db.query.limits",
    "value":{},
    "commit":true
  }'
}
