### Benchmarking

Setup m3db and coordinator:

1) Run m3dbnode

        $ cd $GOPATH/src/github.com/m3db/m3db/
        $ sudo ./bin/m3dbnode -f $GOPATH/src/github.com/m3db/m3db/src/coordinator/benchmark/benchmarker/m3dbnode-local-config.yaml

2) Run m3coordinator

        $ cd $GOPATH/src/github.com/m3db/m3db/src/coordinator
        $ make services
        $ ./bin/m3coordinator --config.file=$GOPATH/src/github.com/m3db/m3db/src/coordinator/benchmark/benchmarker/m3coordinator-local-config.yml

This new benchmarker requires a list of files in opentsdb format. These can be generated using the influxdb-comparisons tool, and may need to be regenerated periodically if the data gets too stale:

1) Pull down benraskin92's fork of `influxdb-comparisons` to generate data:

       $ git clone https://github.com/benraskin92/influxdb-comparisons.git $GOPATH/src/github.com/
       $ cd cmd/bulk_data_gen
       $ go build
       $ cd $GOPATH/src/github.com/m3db/m3db/src/coordinator/benchmark/benchmarker/
       $ ./data_gen.sh

    (NB: the path during these steps and/or in data_gen.sh may not be correct depending on $GOPATH setup)
    This should create a file named benchmark_opentsdb with around 500,000 generated metrics.

2) Build the benchmarker

       $ cd $GOPATH/src/github.com/m3db/m3db/src/coordinator/benchmark/benchmarker/
       $ go build

3) The first time you run the benchmarker, or any time you need to regenerate data (e.g. if generated data is too stale to be written to m3db), add the --regenerateData flag:

        $ ./benchmarker --regenerateData

4) Otherwise, run

        $ ./benchmarker

Notable options for tweaking performance:

        $ --batch=1000 : signifies the number of metrics to send per batch (prometheus defaults to 100, the benchmarker defaults to 1000)
        $ --workers=2 : signifies the number of parallel workers that send encoded metrics to coordinator
        $ --cardinality : if this is set, cardinality for the input file is calculated instead of benchmarking

5) Don't forget to delete all the generated junk before commiting (sorry, will fix this)!
