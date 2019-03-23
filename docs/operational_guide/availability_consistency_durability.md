# Tuning Availability, Consistency, and Durability

## Overview

Generally speaking M3DB is an [H.A](https://en.wikipedia.org/wiki/High_availability) because it doesn't use a consensus protocol like Raft or Paxos to enforce strong consensus and consistency guarantees. However, even within the category of H.A systems, there is a broad spectrum of consistency and durability guarantees that a database can provide. Like many other H.A systems, M3DB allows database operators to tune the consistency and durability guarantees of the system in order to achieve the desired balance between performance, availability, durability, and consistency.

The rest of this document will discuss the various knobs that M3DB allows M3DB operators to tune.

We recommend referring to [this sample M3 YAML configuration file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-all-config.yml) which has every possible configuration value set when reading the rest of this document to see how the described configurations fit into the YAML file as a whole.

### Client Write and Read Consistency

The possible configuration values for write and read consistency are discussed  in more detail [in this section](../m3db/architecture/consistencylevels.md) of the documentation, but in short M3DB behaves similarly to other H.A systems with configurable consistency such as Cassandra that allow the caller to control the consistency level of writes and reads from the client.

The most important thing to understand is that if you want the guarantee that you will be able to read the outcome of every succesful write then you must perform both writes and read with a consistency level of `majority`.

### Commitlog Configuration

By default M3DB runs with an asynchronous commitlog such that writes will be acknowleged as successful by the client even though the data may not have been physically flushed to the commitlog on disk yet. M3DB supports changing this default behavior to run the commitlog synchronously, but this is not currently exposed to users in the YAML configuration and generally leads to a massive performance degradation.

M3DB operators can control when M3DB will attempt to flush the commitlog via the `commitlog` section of the YAML configuration. For example the default configuration:

```
commitlog:
	flushMaxBytes: 524288
	flushEvery: 1s
	queue:
			calculationType: fixed
			size: 2097152
```

states that the commitlog should be flushed whenever one of the following is true:

1. 524288 or more bytes have been written since the last time M3DB flushed the commitlog.
2. One second or more has elapsed since the last time M3DB flushed the commitlog.

In addition, the configuration also states that M3DB should allow up to `2097152` writes to be buffered in the commitlog queue before the database node will begin rejecting incoming writes until it can begin to drain the queue and catch up. Increasing the size of this queue can often increase the write throughput of an M3DB node at the cost of potentially losing more data if the node experiences a sudden failure like a hard crash or power loss.

### Writing New Series Asynchronously

The default M3DB YAML configuration will contain the following as a top-level key under the `db` section:

```
writeNewSeriesAsync: true
```

This instructs M3DB to handle writes for new timeseries (for a given time block) asynchronously. Creating a new timeseries in memory is much more expensive than simply appending a new write to an existing series, so the default configuration of creating them asynchronously improve's M3DBs write throughput significantly when many new series are being created all at once.

However, since new time series are created asynchronously, its possible that there may be a brief delay inbetween when a write is acknowledged by the client and when that series becomes available for subsequent reads.

M3DB also allows operators to ratelimit the number of new series that can be created per second via the following configuration:

```
writeNewSeriesLimitPerSecond: 1048576
```

This value can be set much lower than the default value for workloads in which a significant increase in cardinality usually indicates an abusive caller.

### Ignoring Corrupt Commitlogs on Bootstrap

If M3DB is shutdown gracefully via a sigterm signal, it will ensure that all pending writes are flushed to the commitlog on disk before the process exists. However, in situations where the process crashed/exited unexpectedly or the node itself experienced a sudden failure, the tail end of the commitlog may be corrupt.

In that situation M3DB will read as much of the commitlog as its able to in order to recover as much data as possible, however it then needs to make a decision. It can either come up successfully and tolerate the minor amount of data loss or it can attempt to stream the missing data from its peers. This behavior is controlled by the following default configuration:

```
bootstrap:
	commitlog:
		returnUnfulfilledForCorruptCommitLogFiles: false
```

In the situation where only a single node fails, the optimal outcome is for the node to attempt to repair itself from one of its peers, however, if a quorum of nodes fail and encounter corrupt commitlog files then they will get stuck in a deadlock attempting to stream data from each other with neither able to make any progress due to the fact that they cannot achieve quorum. This issue requires an operator with significant M3DB operational experience to step in and resolve, so we recommend running with the default configuration set to `false` to avoid this issue altogether since in most cases a small amount of data loss is preferable to a quorum of nodes that crash and fail to start back up automatically.
