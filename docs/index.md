# M3DB, a distributed time series database

These documents provide documentation on the overview and operational guidelines for M3DB.

## About

M3DB, inspired by [Gorilla][gorilla] and [InfluxDB][influxdb], is a distributed time series database released as open source by [Uber Technologies][ubeross]. It can be used for storing realtime metrics at long retention:

* Distributed hybrid in-memory and disk time series storage
* Cluster management built on top of [etcd][etcd]
* M3TSZ float64 compression inspired by Gorilla TSZ compression
* Arbitrary time precision down to nanosecond precision with the ability to switch precision with any write
* Configurable out of order writes

## Current Limitations

Due to the nature of the requirements for the project, which are primarily to reduce the cost of ingesting and storing billions of timeseries and provide fast scalable reads, there are a few limitations currently that make M3DB not suitable for use as a general purpose time series database.

The project has aimed to avoid compactions when at all possible, currently the only compactions that occur are in memory for the current mutable compressed time series block (default configured at 2 hours). As such out of order writes are limited to the size of a single compressed time series block, and consequently means backfilling large amounts of data is not currently possible.

The project has also has optimized for the storage and retrieval of float64 values, as such there is no way to use it as a general time series database of arbitrary data structures just yet.

[gorilla]: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
[influxdb]: https://github.com/influxdata/influxdb
[etcd]: https://github.com/coreos/etcd
[ubeross]: http://uber.github.io
