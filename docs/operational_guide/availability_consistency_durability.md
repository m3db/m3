# Tuning Availability, Consistency, and Durability

## Overview

M3DB is designed as a High Availability [HA](https://en.wikipedia.org/wiki/High_availability) system because it doesn't use a consensus protocol like Raft or Paxos to enforce strong consensus and consistency guarantees.
However, even within the category of HA systems, there is a broad spectrum of consistency and durability guarantees that a database can provide.
To address as many use cases as possible, M3DB can be tuned to achieve the desired balance between performance, availability, durability, and consistency.

Generally speaking, [the default and example configuration for M3DB](https://github.com/m3db/m3/tree/master/src/dbnode/config) favors performance and availability, as that is well-suited for M3DB's most common metrics and Observability use cases. To instead favor consistency and durability, consider tuning values as described in (TODO).
Database operators who are using M3DB for workloads that require stricter consistency and durability guarantees should consider tuning the default configuration to better suite their use case.
The rest of this document describes the various configuration options that are available to M3DB operators to make those tradeoffs.

While reading the rest of this document, we recommend referring to [the default configuration file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-all-config.yml) (which has every possible configuration value set) to see how the described values fit into M3DB's configuration as a whole.

### Client Write and Read Consistency

The possible configuration values for write and read consistency are discussed in more detail in [the Consistency Levels section](../m3db/architecture/consistencylevels.md). In short, M3DB behaves similarly to other HA systems with configurable consistency such as Cassandra that allow the caller to control the consistency level of writes and reads from the client.

The most important thing to understand is that **if you want to guarantee that you will be able to read the result of every successful write, then both writes and reads must be done with `majority` consistency.**

### Commitlog Configuration

By default M3DB runs with an asynchronous commitlog such that writes will be reported as successful by the client, though the data may not have been flushed to disk yet.
M3DB supports changing this default behavior to run the commitlog synchronously, but this is not currently exposed to users in the YAML configuration and generally leads to a massive performance degradation.

M3DB operators can control when M3DB will attempt to flush the commitlog via the `commitlog` section of the YAML configuration.
For example, consider the default configuration:

```
commitlog:
  flushMaxBytes: 524288
  flushEvery: 1s
  queue:
    calculationType: fixed
    size: 2097152
```

This configuration states that the commitlog should be flushed whenever either of the following is true:

1. 524288 or more bytes have been written since the last time M3DB flushed the commitlog.
2. One or more seconds has elapsed since the last time M3DB flushed the commitlog.

In addition, the configuration also states that M3DB should allow up to `2097152` writes to be buffered in the commitlog queue before the database node will begin rejecting incoming writes so it can attempt to drain the queue and catch up. Increasing the size of this queue can often increase the write throughput of an M3DB node at the cost of potentially losing more data if the node experiences a sudden failure like a hard crash or power loss.

### Writing New Series Asynchronously

The default M3DB YAML configuration will contain the following as a top-level key under the `db` section:

```
writeNewSeriesAsync: true
```

This instructs M3DB to handle writes for new timeseries (for a given time block) asynchronously. Creating a new timeseries in memory is much more expensive than simply appending a new write to an existing series, so the default configuration of creating them asynchronously improves M3DBs write throughput significantly when many new series are being created all at once.

However, since new time series are created asynchronously, its possible that there may be a brief delay inbetween when a write is acknowledged by the client and when that series becomes available for subsequent reads.

M3DB also allows operators to ratelimit the number of new series that can be created per second via the following configuration:

```
writeNewSeriesLimitPerSecond: 1048576
```

This value can be set much lower than the default value for workloads in which a significant increase in cardinality usually indicates an abusive caller.

### Ignoring Corrupt Commitlogs on Bootstrap

If M3DB is shuts down gracefully (i.e via SIGTERM), it will ensure that all pending writes are flushed to the commitlog on disk before the process exists.
However, in situations where the process crashed/exited unexpectedly or the node itself experienced a sudden failure, the tail end of the commitlog may be corrupt.
In such situations, M3DB will read as much of the commitlog as possible in an attempt to recover the maximum amount of data. However, it then needs to make a decision: it can either **(a)** come up successfully and tolerate an ostensibly minor amount of data or loss, or **(b)** attempt to stream the missing data from its peers.
In that situation M3DB will read as much of the commitlog as its able to in order to recover as much data as possible, however it then needs to make a decision. It can either come up successfully and tolerate the minor amount of data loss or it can attempt to stream the missing data from its peers. This behavior is controlled by the following default configuration:

```
bootstrap:
  commitlog:
    returnUnfulfilledForCorruptCommitLogFiles: false
```

In the situation where only a single node fails, the optimal outcome is for the node to attempt to repair itself from one of its peers. However, if a quorum of nodes fail and encounter corrupt commitlog files, they will deadlock while attempting to stream data from each other, as no nodes will be able to make progress due to a lack of quorum.
This issue requires an operator with significant M3DB operational experience to manually bootstrap the cluster; thus the official recommendation is to set `returnUnfulfilledForCorruptCommitLogFiles: false` to avoid this issue altogether. In most cases, a small amount of data loss is preferable to a quorum of nodes that crash and fail to start back up automatically.
