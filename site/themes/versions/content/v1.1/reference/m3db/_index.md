---
title: "M3DB, a distributed time series database"
menuTitle: "M3DB"
weight: 4
chapter: true
---

## About

M3DB, inspired by [Gorilla][gorilla] and [Cassandra][cassandra], is a distributed time series database released as open source by [Uber Technologies][ubeross]. It can be used for storing realtime metrics at long retention.

Here are some attributes of the project:

-   Distributed time series storage, single nodes use a WAL commit log and persists time windows per shard independently
-   Cluster management built on top of [etcd][etcd]
-   Built-in synchronous replication with configurable durability and read consistency (one, majority, all, etc)
-   M3TSZ float64 compression inspired by Gorilla TSZ compression, configurable as lossless or lossy
-   Arbitrary time precision configurable from seconds to nanoseconds precision, able to switch precision with any write
-   Configurable out of order writes, currently limited to the size of the configured time window's block size

## Current Limitations

Due to the nature of the requirements for the project, which are primarily to reduce the cost of ingesting and storing billions of timeseries and providing fast scalable reads, there are a few limitations currently that make M3DB not suitable for use as a general purpose time series database.

The project has aimed to avoid compactions when at all possible, currently the only compactions M3DB performs are in-memory for the mutable compressed time series window (default configured at 2 hours).  As such out of order writes are limited to the size of a single compressed time series window.  Consequently backfilling large amounts of data is not currently possible.

The project has also optimized the storage and retrieval of float64 values, as such there is no way to use it as a general time series database of arbitrary data structures just yet.

[gorilla]: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

[cassandra]: http://cassandra.apache.org/

[etcd]: https://github.com/etcd-io/etcd

[ubeross]: http://uber.github.io
