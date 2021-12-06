#!/usr/bin/env bash

set -ex
source "$M3_PATH"/scripts/docker-integration-tests/common.sh
t=$(date +%s)

function write_metrics {
  NUM=$1
  EXTRA=${2:-default}
  echo "Writing $NUM metrics to [0.0.0.0:9003]"
  set +x
  for (( i=0; i<$NUM; i++ ))
  do
    curl -X POST 0.0.0.0:9003/writetagged -d '{
      "namespace": "unagg",
      "id": "{__name__=\"'$METRIC_NAME'\",'$EXTRA'=\"extra\",val=\"'$i'\"}",
      "tags": [
        {
          "name": "__name__",
          "value": "'$METRIC_NAME'"
        },
        { 
          "name": "'$EXTRA'",
          "value": "extra"
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
  ACTUAL=$(echo $RESPONSE | jq .data.result[].metric.foo | sort | tr -d "\n")
  CONCAT=$(echo $EXPECTED | tr -d " ")
  test $ACTUAL_COUNT = $EXPECTED_COUNT && test $ACTUAL = $CONCAT
}

function test_replace { 
  METRIC_NAME="quail_$t"
  write_metrics 5
  sleep 1
  query='label_replace('$METRIC_NAME',"foo","bar_$1","val","(.*)")'
  test_instantaneous $query 5 "\"bar_0\" \"bar_1\" \"bar_2\" \"bar_3\" \"bar_4\""
  query='label_replace('$METRIC_NAME',"foo","bar_$1","val","(.*)")-0'
  test_instantaneous $query 5 "\"bar_0\" \"bar_1\" \"bar_2\" \"bar_3\" \"bar_4\""
}

function test_exists {
  QUERY=$1
  EXPECTED_EXISTS=$2
  EXPECTED_NOT_EXISTS=$3
  EXPECTED_COUNT=$4
  RESPONSE=$(curl -sSL "http://localhost:7201/api/v1/query?query=$METRIC_NAME\{$QUERY\}")
  ACTUAL_COUNT_EXISTS=$(echo $RESPONSE | jq .data.result[].metric.$EXPECTED_EXISTS | grep extra | wc -l)
  ACTUAL_COUNT_NOT_EXISTS=$(echo $RESPONSE | jq .data.result[].metric.$EXPECTED_NOT_EXISTS | grep extra | wc -l)
  test $ACTUAL_COUNT_EXISTS = $EXPECTED_COUNT && test $ACTUAL_COUNT_NOT_EXISTS = 0
}

function test_empty_matcher {
  export METRIC_NAME="foo_$t"
  write_metrics 5 exists
  write_metrics 5 not_exists
 
  retry_with_backoff ATTEMPTS=3 TIMEOUT=1 test_exists 'not_exists=\"\"' exists not_exists 5
  retry_with_backoff ATTEMPTS=3 TIMEOUT=1 test_exists 'not_exists!=\"\"' not_exists exists 5

  retry_with_backoff ATTEMPTS=3 TIMEOUT=1 test_exists 'exists=\"\"' not_exists exists 5
  retry_with_backoff ATTEMPTS=3 TIMEOUT=1 test_exists 'exists!=\"\"' exists not_exists 5
}

function test_parse_threshold {
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

function test_duplicates {
  now=$(date +"%s")
  start=$(( $now - 100 ))
  end=$(( $now + 100 ))
  QUERY="query=$METRIC_NAME&start=$start&end=$end&format=json"
  ACTUAL=$(curl "localhost:7201/api/v1/prom/remote/read?$QUERY" | jq .[][].series[].tags[])
  EXPECTED=$(echo '[ "__name__", "'${METRIC_NAME}'" ] [ "val", "extra" ] [ "val", "0" ]' | jq)
  test "$ACTUAL"="$EXPECTED"
}

function test_debug_prom_returns_duplicates {
  export METRIC_NAME="duplicate_$t"
  # NB: this writes metrics of the form `duplicate_t{val="extra", val="1"}`
  #     with a duplicated `val` tag.
  write_metrics 1 val
  retry_with_backoff ATTEMPTS=3 TIMEOUT=1 test_duplicates
}

function test_correctness {
  test_parse_threshold
  test_replace 
  test_empty_matcher

  test_debug_prom_returns_duplicates
}
