### Benchmarking

To benchmark m3db writes using m3coordinator.

1) Make sure you have the lastest version of m3coordinator
2) Pull down benraskin92's fork of `influxdb-comparisons`:

       $ git clone https://github.com/benraskin92/influxdb-comparisons.git $GOPATH/src/github.com/
       $ cd cmd/bulk_data_gen
       $ go build

3) Create metrics - from the `github.com/m3db/m3coordinator/benchmark/data` directory, run:

       $ ./data_gen.sh

> Note: If you need to adjust the script to get more metrics, make sure the timestamp start and end are within the `buffer_past` config that is set for m3db and the current time, otherwise you will get datapoint too far in the past/future errors.

4) Start m3db. You can use the config that is in `github.com/m3db/m3coordinator/benchmark/`

        $ ./bin/m3dbnode -f $GOPATH/src/github.com/m3db/m3coordinator/benchmark/m3db_config.yaml

5) Build and run the benchmark tool in m3coordinator

        $ cd $GOPATH/src/github.com/m3db/m3coordinator/benchmark/
        $ go build
        $ ./write -data-file=$GOPATH/src/github.com/influxdb-comparisons/cmd/bulk_data_gen/benchmark_opentsdb -workers=2000
