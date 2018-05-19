#!/bin/sh

# This script is used to generate metrics data to be used for benchmarking.
# It uses a modified version of InfluxDB's `bulk_data_gen` tool (see the README for more info).
timeNow=$(date +%s)
startSeconds=`expr $timeNow - 1000`
endSeconds=`expr $timeNow - 10`
startSeconds=`expr $startSeconds + 12000`
endSeconds=`expr $endSeconds + 12000`
start=`date -r $startSeconds "+%FT%T%Z" | sed s/+// | sed s/EST/Z/ | sed s/EDT/Z/`
end=`date -r $endSeconds "+%FT%T%Z" | sed s/+// | sed s/EST/Z/ | sed s/EDT/Z/`

seedOne=519129548
seedTwo=925110754
seedThree=228803099
seed=$seedOne

echo "start: $start"
echo "end: $end"

$GOPATH/src/github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen/bulk_data_gen -format=opentsdb -timestamp-start=$start -timestamp-end=$end -scale-var=500 -seed=$seed > benchmark_opentsdb
