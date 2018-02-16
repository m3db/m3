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

4a) For a three-node m3db cluster on GCP:

    1. Download necessary packages
        $ sudo apt-get install golang golint make git golang-glide
    2. Setup paths and clone m3db
        $ export GOPATH=$HOME/code
        $ mkdir -p $GOPATH/src/github.com/m3db
        $ cd $GOPATH/src/github.com/m3db
        $ git clone https://github.com/m3db/m3db
    3. Build m3db
        $ git submodule update --init --recursive
        $ glide install
        $ make services
    4. There are three config files in the `$GOPATH/src/github.com/m3db/m3coordinator/benchmark/configs/multi_node_setup` directory (`m3dbnode-server1-config.yaml`, `m3dbnode-server2-config.yaml`, `m3dbnode-server3-config.yaml`) - you will use one config per GCP instance
    5. Update config on each host to reflect the correct IP addresses of the GCP instances and the names of the m3db servers. NB: this example is for a three node m3db cluster.
        - You should only need to update the IP addresses for the topology under the config section. e.g.:
            ```
            config:
              static:
                  topology:
                      shards: 64
                      hosts:
                          - host: m3db_server_1
                            listenAddress: "10.142.0.6:9000"
                          - host: m3db_server_2
                            listenAddress: "10.142.0.7:9000"
                          - host: m3db_server_3
                            listenAddress: "10.142.0.9:9000"
                  namespaces:
                      - name: metrics
                        options:
                        retention:
                          retentionPeriod: 24h
                          blockSize: 4h
                          bufferPast: 2h
            ```
    6. Run m3db:
        $ sudo ./bin/m3dbnode -f $GOPATH/src/github.com/m3db/m3coordinator/benchmark/configs/multi_node_setup/m3dbnode-server[1-3]-config.yaml

5) Build and run the benchmark tool in m3coordinator

        $ cd $GOPATH/src/github.com/m3db/m3coordinator/benchmark/
        $ go build
        $ ./write -data-file=$GOPATH/src/github.com/influxdb-comparisons/cmd/bulk_data_gen/benchmark_opentsdb -workers=2000
