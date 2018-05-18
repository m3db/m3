m3nsch [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]
======
m3nsch (pronounced `mensch`) is a load testing tool for M3DB. It has two components:
  - `m3nsch_server`: long lived process which does the load generation
  - `m3nsch_client`: cli wrapper which controls the myriad `m3nsch_server(s)`

A typical deploy will have multiple hosts, each running a single `m3nsch_server` instance,
and a single `m3nsch_client` used to control them.

### Build
```
$ make prod-m3nsch_server
$ make prod-m3nsch_client
$ cat <<EOF > server-conf.yaml
server:
  listenAddress: "0.0.0.0:12321"
  debugAddress: "0.0.0.0:13441"
  cpuFactor: 0.9

metrics:
  sampleRate: 0.1
  m3:
    hostPort: "<host-port-for-tally>"
    service: "m3nsch"
    includeHost: true
    env: development

m3nsch:
  concurrency: 2000
  numPointsPerDatum: 60

# any additional configs you may have
EOF
```

### Deploy
(1) Transfer the `m3nsch_server` binary, and `server-conf.yaml` to all the hosts to be used to generate hosts, e.g. Ingesters

(2) On each host from (1), kick of the server process by running:
```
./m3nsch_server -f server-conf.yaml
```

(3) Transfer `m3nsch_client` binary to a host with network connectivity to all the hosts.

### Sample Usage
```
# set an env var containing the host endpoints seperated by commas
$ export ENDPOINTS="host1:12321,host2:12321"

# get the status of the various endpoints, make sure all are healthy
$ ./m3nsch_client --endpoints $ENDPOINTS status

# investigate the various options available during initialization
$ ./m3nsch_client init --help
...
Flags:
  -b, --basetime-offset duration   offset from current time to use for load, e.g. -2m, -30s (default -2m0s)
  -c, --cardinality int            aggregate workload cardinality (default 10000)
  -f, --force                      force initialization, stop any running workload
  -i, --ingress-qps int            aggregate workload ingress qps (default 1000)
  -p, --metric-prefix string       prefix added to each metric (default "m3nsch_")
  -n, --namespace string           target namespace (default "testmetrics")
  -v, --target-env string          target env for load test (default "test")
  -z, --target-zone string         target zone for load test (default "sjc1")
  -t, --token string               [required] unique identifier required for all subsequent interactions on this workload

Global Flags:
  -e, --endpoints stringSlice   host:port for each of the agent process endpoints

# initialize the servers, set any workload parameters, target env/zone
# the command below targets the production sjc1 m3db cluster with each `m3nsch_server` attempting to
# sustain a load of 100K writes/s from a set of 1M metrics
$ ./m3nsch_client --endpoints $ENDPOINTS init \
  --token prateek-sample \
  --target-env prod      \
  --target-zone sjc1     \
  --ingress-qps 100000   \
  --cardinality 1000000  \

# start the load generation
$ ./m3nsch_client --endpoints $ENDPOINTS start

# modifying the running load uses many of the same options as `init`
$ ./m3nsch_client modify --help
...
Flags:
  -b, --basetime-offset duration   offset from current time to use for load, e.g. -2m, -30s (default -2m0s)
  -c, --cardinality int            aggregate workload cardinality (default 10000)
  -i, --ingress-qps int            aggregate workload ingress qps (default 1000)
  -p, --metric-prefix string       prefix added to each metric (default "m3nsch_")
  -n, --namespace string           target namespace (default "testmetrics")

Global Flags:
  -e, --endpoints stringSlice   host:port for each of the agent process endpoints

# the command below bumps up the workload on each `m3nsch_server`, to sustain
# a load of 1M writes/s from a set of 10M metrics
$ ./m3nsch_client --endpoints $ENDPOINTS
  --ingress-qps 1000000   \
  --cardinality 10000000  \

# finally, stop the load
$ ./m3nsch_client --endpoints $ENDPOINTS stop

# probably want to teardown the running server processes on the various hosts
```

<hr>

This project is released under the [Apache License, Version 2.0](LICENSE).

[doc-img]: https://godoc.org/github.com/m3db/m3nsch?status.svg
[doc]: https://godoc.org/github.com/m3db/m3nsch
[ci-img]: https://travis-ci.org/m3db/m3nsch.svg?branch=master
[ci]: https://travis-ci.org/m3db/m3nsch
[cov-img]: https://coveralls.io/repos/m3db/m3nsch/badge.svg?branch=master&service=github
[cov]: https://coveralls.io/github/m3db/m3nsch?branch=master
