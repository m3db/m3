---
title: "Overview"
weight: 1
---

M3DB is written entirely in Go and does not have any required dependencies. For larger deployments, one may use an etcd cluster to manage M3DB cluster membership and topology definition.

## High Level Goals

Some of the high level goals for the project are defined as:

-   **Monitoring support:** M3DB was primarily developed for collecting a high volume of monitoring time series data, distributing the storage in a horizontally scalable manner and most efficiently leveraging the hardware.  As such time series that are not read frequently are not kept in memory.
-   **Highly configurable:** Provide a high level of configuration to support a wide set of use cases and runtime environments.
-   **Variable durability:** Providing variable durability guarantees for the write and read side of storing time series data enables a wider variety of applications to use M3DB. This is why replication is primarily synchronous and is provided with configurable consistency levels, to enable consistent writes and reads. It must be possible to use M3DB with strong guarantees that data was replicated to a quorum of nodes and that the data was durable if desired.

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
