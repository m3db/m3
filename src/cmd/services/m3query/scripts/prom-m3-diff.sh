#!/bin/bash
# This script can be used to run the same query on prometheus as well as m3.
# Sample usage: bash prom-m3-diff.sh "count_over_time(go_gc_duration_seconds[30s])" 1535125074
target=${1?"Missing target, usage: $0 target start"}
start=${2?"Missing start, usage: $0 target start"}
duration=${3:-1000}
step=${4:-"15s"}
end=$((start + duration))
m3port="localhost:7201"
promport="localhost:9090"
curl -fsS $promport/status > /dev/null || { echo "Prom port not open";  exit 1; }
curl -fsS $m3port/health > /dev/null || { echo "M3Query port not open";  exit 1; }
m3command="$m3port/api/v1/prom/native/read?start=$start&end=$end&step=$step&debug=true --data-urlencode target=$target"
promcommand="$promport/api/v1/query_range?start=$start&end=$end&step=$step --data-urlencode query=$target"
echo $m3command
echo $promcommand
curl -G $m3command > m3out
curl -G $promcommand > promout
jq ".[]|.tags,.datapoints" m3out > m3result
jq ".data.result|.[]|.metric,.values" promout > promresult
echo "M3 file size" $(stat -f%z m3out)
echo "Prom file size" $(stat -f%z promout)
