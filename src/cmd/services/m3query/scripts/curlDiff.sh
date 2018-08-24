#!/bin/bash
# This script can be used to compare prometheus query output with m3query output
# Sample usage: bash curlDiff.sh "count_over_time(go_gc_duration_seconds[30s])" 1535125074
target=${1?"Missing target, usage: $0 target start"}
start=${2?"Missing start, usage: $0 target start"}
duration=${3:1000}
end=$((start + duration))
m3command="localhost:7201/api/v1/prom/native/read?start=$start&end=$end&step=15s&debug=true --data-urlencode target=$target"
promcommand="localhost:9090/api/v1/query_range?start=$start&end=$end&step=15s --data-urlencode query=$target"
echo $m3command
echo $promcommand
curl -G  $m3command > m3out
curl -G $promcommand > promout
jq ".[]|.tags,.datapoints" m3out > m3result
jq ".data.result|.[]|.metric,.values" promout > promresult
