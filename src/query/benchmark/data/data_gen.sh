#!/bin/sh

# This script is used to generate metrics data to be used for benchmarking.
# It uses a modified version of InfluxDB's `bulk_data_gen` tool (see the README for more info).
# NB (braskin): There are several lines which are specific to running the benchmark on GCP.

# on GCP:
# export GOPATH=/home/nikunj/code

timeNow=$(date +%s)
startSeconds=`expr $timeNow - 1000`
endSeconds=`expr $timeNow - 10`

# for GCP use:
# start=`date -d @$startSeconds +"+%FT%T%Z" | sed s/+// | sed s/UTC/Z/`
# end=`date -d @$endSeconds +"+%FT%T%Z" | sed s/+// | sed s/UTC/Z/`

# for local (mac) use:
startSeconds=`expr $startSeconds + 18000`
endSeconds=`expr $endSeconds + 18000`
start=`date -r $startSeconds "+%FT%T%Z" | sed s/+// | sed s/EST/Z/`
end=`date -r $endSeconds "+%FT%T%Z" | sed s/+// | sed s/EST/Z/`

seedOne=519129548
seedTwo=925110754
seedThree=228803099
seed=$seedOne

echo "start: $start"
echo "end: $end"

$GOPATH/src/github.com/influxdb-comparisons/cmd/bulk_data_gen/bulk_data_gen -timestamp-start=$start -timestamp-end=$end -scale-var=1000 -seed=$seed > $GOPATH/src/github.com/influxdb-comparisons/cmd/bulk_data_gen/benchmark_influx && $GOPATH/src/github.com/influxdb-comparisons/cmd/bulk_data_gen/bulk_data_gen -format=opentsdb -timestamp-start=$start -timestamp-end=$end -scale-var=1000 -seed=$seed > $GOPATH/src/github.com/influxdb-comparisons/cmd/bulk_data_gen/benchmark_opentsdb

# uncomment to run actual benchmark (GCP)
# ./benchmark -workers=2000 -data-file=$GOPATH/src/github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen/benchmark_opentsdb -cpuprofile=false -batch=5000 -address="0.0.0.0:8000" -benchmarkers="10.142.0.2:8000,10.142.0.4:8000,10.142.0.5:8000"
