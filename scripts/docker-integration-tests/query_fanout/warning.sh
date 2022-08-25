#!/usr/bin/env bash

set -ex
source "$M3_PATH"/scripts/docker-integration-tests/common.sh

COMPOSE_FILE="$M3_PATH"/scripts/docker-integration-tests/query_fanout/docker-compose.yml
HEADER_FILE=headers.out

function write_metrics {
  CLUSTER=$1
  case $CLUSTER in
    coordinator-cluster-a )
      PORT=9003 ;;
    coordinator-cluster-b )
      PORT=19003 ;;
    coordinator-cluster-c )
      PORT=29003 ;;
    *)
      echo $CLUSTER "is not a valid coordinator cluster"
      exit 1
  esac

  NUM=$2
  echo "Writing $NUM metrics to $1 [0.0.0.0:$PORT]"
  set +x
  for (( i=0; i<$NUM; i++ ))
  do
    curl -X POST 0.0.0.0:$PORT/writetagged -d '{
      "namespace": "unagg",
      "id": "{__name__=\"'$METRIC_NAME'\",cluster=\"'$CLUSTER'\",val=\"'$i'\"}",
      "tags": [
        {
          "name": "__name__",
          "value": "'$METRIC_NAME'"
        },
        {
          "name": "cluster",
          "value": "'$CLUSTER'"
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

function clean_headers {
  rm $HEADER_FILE
}

function test_instant_query {
  LIMIT=$1
  EXPECTED=$2
  ENDPOINT=$3||""
  EXPECTED_HEADER=$4||""
  trap clean_headers EXIT
  RESPONSE=$(curl -sSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive: false" \
    "http://0.0.0.0:7201${ENDPOINT}/api/v1/query?query=count($METRIC_NAME)")
  ACTUAL=$(echo $RESPONSE | jq .data.result[0].value[1] | tr -d \" | tr -d \')
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  test "$ACTUAL" = "$EXPECTED"
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}

t=$(date +%s)

function test_range_query {
  LIMIT=$1
  EXPECTED=$2
  EXPECTED_HEADER=$3
  trap clean_headers EXIT

  start=$t
  end=$(($start+9))

  RESPONSE=$(curl -sSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive: false" \
    "http://0.0.0.0:7201/api/v1/query_range?start=$start&end=$end&step=10&query=count($METRIC_NAME)")
  ACTUAL=$(echo $RESPONSE | jq .data.result[0].values[0][1] | tr -d \" | tr -d \')
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  test "$ACTUAL" = "$EXPECTED" 
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}

function test_search {
  s=$(( $(date +%s) - 60 ))
  start=$(date -r $s +%Y-%m-%dT%H:%M:%SZ)
  e=$(( $(date +%s) + 60 ))
  end=$(date -r $e +%Y-%m-%dT%H:%M:%SZ)
  curl -D headers -X POST 0.0.0.0:7201/search -d '{
    "start": "'$start'",
    "end": "'$end'",
    "matchers": [
        {
          "type": 0,
          "name":"'$(echo __name__ | base64)'",
          "value":"'$(echo $METRIC_NAME | base64)'"
        }
      ]
    }
  '

  LIMIT=$1
  EXPECTED_HEADER=$2
  trap clean_headers EXIT

   curl -sSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive: false" \
    "http://0.0.0.0:7201/api/v1/search?query=val:.*"
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}

function test_labels {
  LIMIT=$1
  EXPECTED_HEADER=$2
  trap clean_headers EXIT

   curl -sSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive: false" \
    "http://0.0.0.0:7201/api/v1/labels"
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}

function test_match {
  LIMIT=$1
  EXPECTED=$2
  EXPECTED_HEADER=$3
  trap clean_headers EXIT
  RESPONSE=$(curl -gsSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive: false" \
    "http://0.0.0.0:7201/api/v1/series?match[]=$METRIC_NAME")

  ACTUAL=$(echo $RESPONSE | jq '.data | length')
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  # NB: since it's not necessarily deterministic which series we get back from
  # remote sources, check that we cot at least EXPECTED values, which will be
  # the bottom bound.
  test "$ACTUAL" -ge "$EXPECTED"
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}

function test_label_values {
  LIMIT=$1
  EXPECTED_HEADER=$2
  trap clean_headers EXIT

   curl -sSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive: false" \
    "http://0.0.0.0:7201/api/v1/label/val/values"
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}
 
function write_carbon {
  CLUSTER=$1
  case $CLUSTER in
    coordinator-cluster-a )
      PORT=7204 ;;
    coordinator-cluster-b )
      PORT=17204 ;;
    coordinator-cluster-c )
      PORT=27204 ;;
    *)
      echo $CLUSTER "is not a valid coordinator cluster"
      exit 1
  esac

  NUM=$2
  echo "Writing $NUM metrics to $1 [0.0.0.0:$PORT]"
  set +x
  for (( i=0; i<$NUM; i++ ))
  do
    echo "$GRAPHITE.$i.$CLUSTER 1 $t" | nc 0.0.0.0 $PORT
  done
  set -x
}

function render_carbon {
  LIMIT=$1
  EXPECTED=$2
  EXPECTED_HEADER=$3
  trap clean_headers EXIT 

  start=$(($t))
  end=$(($start+200))
  RESPONSE=$(curl -sSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive:false" \
    "http://localhost:7201/api/v1/graphite/render?target=countSeries($GRAPHITE.*.*)&from=$start&until=$end")
  ACTUAL=$(echo $RESPONSE | jq .[0].datapoints[0][0])
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  test "$ACTUAL" = "$EXPECTED"
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}

function find_carbon {
  LIMIT=$1
  EXPECTED_HEADER=$2
  trap clean_headers EXIT

  RESPONSE=$(curl -sSL -D $HEADER_FILE -H "M3-Limit-Max-Series:$LIMIT" -H "M3-Limit-Require-Exhaustive: false" \
    "http://localhost:7201/api/v1/graphite/metrics/find?query=$GRAPHITE.*")
  ACTUAL_HEADER=$(cat $HEADER_FILE | grep M3-Results-Limited | cut -d' ' -f2 | tr -d "\r\n")
  test "$ACTUAL_HEADER" = "$EXPECTED_HEADER"
}

function test_fanout_warning_fetch {
  METRIC_NAME="foo_$t"
  export INSTANT_NAME=$METRIC_NAME
  # # write 5 metrics to cluster a
  write_metrics coordinator-cluster-a 5
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 5 "/m3query"
  # limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 4 4 "/m3query" max_fetch_series_limit_applied
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 5 "/prometheus"
  # limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 4 4 "/prometheus" max_fetch_series_limit_applied

  # write 10 metrics to cluster b
  write_metrics coordinator-cluster-b 10
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 15 "/m3query"
  # remote limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 9 14 "/m3query" max_fetch_series_limit_applied
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 15 "/prometheus"
  # remote limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 9 14 "/prometheus" max_fetch_series_limit_applied
}

function test_fanout_warning_fetch_instantaneous {
  METRIC_NAME="bar_$t"
  export RANGE_NAME=$METRIC_NAME
  # write 5 metrics to cluster a
  write_metrics coordinator-cluster-a 5
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_range_query 100 5
  # limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_range_query 4 4 max_fetch_series_limit_applied

  # write 10 metrics to cluster b
  write_metrics coordinator-cluster-b 10
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_range_query 100 15
  # remote limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_range_query 9 14 max_fetch_series_limit_applied
}

function test_fanout_warning_search {
  METRIC_NAME="baz_$t"
  export SEARCH_NAME=$METRIC_NAME
  # write 5 metrics to cluster a
  write_metrics coordinator-cluster-a 5
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_search 1000
  # limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_search 4 max_fetch_series_limit_applied

  # write 10 metrics to cluster b
  write_metrics coordinator-cluster-b 10
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_search 1000
  # remote limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_search 4 max_fetch_series_limit_applied
}

function test_fanout_warning_match {
  METRIC_NAME="qux_$t"
  export MATCH_NAME=$METRIC_NAME
  # write 5 metrics to cluster a
  write_metrics coordinator-cluster-a 5
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_match 6 5
  # limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_match 4 4 max_fetch_series_limit_applied

  # write 10 metrics to cluster b
  write_metrics coordinator-cluster-b 10
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_match 11 10
  # remote limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_match 9 9 max_fetch_series_limit_applied
}

function test_fanout_warning_labels {
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_labels 100
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_labels 1 max_fetch_series_limit_applied
}

function test_fanout_warning_label_values {
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_label_values 100
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_label_values 1 max_fetch_series_limit_applied
}

function test_fanout_warning_fetch_id_mismatch {
  METRIC_NAME="foo_$t"
  export INSTANT_NAME=$METRIC_NAME
  # # write 5 metrics to cluster a
  write_metrics coordinator-cluster-a 5
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 5 "/m3query"
  # limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 4 4 "/m3query" max_fetch_series_limit_applied
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 5 "/prometheus"
  # limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 4 4 "/prometheus" max_fetch_series_limit_applied

  # write 10 metrics to cluster b
  write_metrics coordinator-cluster-b 10
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 15 "/m3query"
  # remote limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 9 14 "/m3query" max_fetch_series_limit_applied
  # unlimited query against cluster a has no header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 15 "/prometheus"
  # remote limited query against cluster a has header
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 9 14 "/prometheus" max_fetch_series_limit_applied
}

function test_fanout_warning_graphite {
  # Update write time as it will otherwise not be written correctly.
  t=$(date +%s)
  # write 5 metrics to cluster a
  write_carbon coordinator-cluster-a 5
  # unlimited query against cluster a has no header
  ATTEMPTS=8 TIMEOUT=1 retry_with_backoff render_carbon 6 5
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff find_carbon 6
  # limited query against cluster a has header
  ATTEMPTS=8 TIMEOUT=1 retry_with_backoff render_carbon 4 4 max_fetch_series_limit_applied
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff find_carbon 4 max_fetch_series_limit_applied

  # Update write time as it will otherwise not be written correctly.
  t=$(date +%s)
  # write 10 metrics to cluster b
  write_carbon coordinator-cluster-b 10
  # unlimited query against cluster a has no header
  ATTEMPTS=8 TIMEOUT=1 retry_with_backoff render_carbon 16 15
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff find_carbon 16
  # remote limited query against cluster a has header
  ATTEMPTS=8 TIMEOUT=1 retry_with_backoff render_carbon 9 14 max_fetch_series_limit_applied
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff find_carbon 9 max_fetch_series_limit_applied
}

function verify_range {
  RANGE=$1
  PORT=$2
  EXPECTED=$3
  start=$(( $t - 3000 ))
  end=$(( $t + 3000 ))
  qs="query=sum_over_time($METRIC_NAME[$RANGE])&start=$start&end=$end&step=1s"
  query="http://0.0.0.0:$PORT/prometheus/api/v1/query_range?$qs"
  curl -sSLg -D $HEADER_FILE "$query" > /dev/null
  warn=$(cat $HEADER_FILE | grep M3-Results-Limited | sed 's/M3-Results-Limited: //g')
  warn=$(echo $warn | sed 's/ /_/g')
  test $warn=$EXPECTED
}

function test_fanout_warning_range {
  t=$(date +%s)
  METRIC_NAME="quart_$t"
  curl -X POST 0.0.0.0:9003/writetagged -d '{
    "namespace": "agg",
    "id": "{__name__=\"'$METRIC_NAME'\",cluster=\"coordinator-cluster-a\",val=\"1\"}",
    "tags": [
      {
        "name": "__name__",
        "value": "'$METRIC_NAME'"
      },
      {
        "name": "cluster",
        "value": "coordinator-cluster-a"
      },
      {
        "name": "val",
        "value": "1"
      }
    ],
    "datapoint": {
      "timestamp":'"$t"',
      "value": 1
    }
  }'


  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff verify_range 1s 7201 resolution_larger_than_query_range_range:_1s,_resolutions:_5s
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff verify_range 1s 17201 resolution_larger_than_query_range_range:_1s,_resolutions:_5s
  
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff verify_range 10s 7201
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff verify_range 10s 17201
}

function test_fanout_warning_missing_zone {
  docker-compose-with-defaults -f ${COMPOSE_FILE} stop coordinator-cluster-c

  METRIC_NAME=$INSTANT_NAME
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 100 15 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_instant_query 9 14 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error

  METRIC_NAME=$RANGE_NAME
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_range_query 100 15 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_range_query 9 14 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error

  METRIC_NAME=$SEARCH_NAME
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_search 1000 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_search 4 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error

  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_labels 100 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_labels 1 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error

  METRIC_NAME=$MATCH_NAME
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_match 11 10 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_match 9 9 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error

  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_label_values 100 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff test_label_values 1 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error

  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff render_carbon 16 15 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff render_carbon 9 14 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error
 
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff find_carbon 16 remote_store_cluster-c_fetch_data_error
  ATTEMPTS=3 TIMEOUT=1 retry_with_backoff find_carbon 9 max_fetch_series_limit_applied,remote_store_cluster-c_fetch_data_error

  docker-compose-with-defaults -f ${COMPOSE_FILE} start coordinator-cluster-c
}

function test_fanout_warnings {
  test_fanout_warning_fetch
  test_fanout_warning_fetch_instantaneous
  test_fanout_warning_search
  test_fanout_warning_match
  test_fanout_warning_labels
  test_fanout_warning_label_values
  test_fanout_warning_fetch_id_mismatch
  export GRAPHITE="foo.bar.$t"
  test_fanout_warning_graphite
  test_fanout_warning_range
  test_fanout_warning_missing_zone
}
