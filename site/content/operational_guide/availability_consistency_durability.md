---
title: "Tuning Availability, Consistency, and Durability"
weight: 5
---

M3DB is designed as a High Availability [HA](https://en.wikipedia.org/wiki/High_availability) system because it doesn't use a consensus protocol like Raft or Paxos to enforce strong consensus and consistency guarantees.
However, even within the category of HA systems, there is a broad spectrum of consistency and durability guarantees that a database can provide.
To address as many use cases as possible, M3DB can be tuned to achieve the desired balance between performance, availability, durability, and consistency.

Generally speaking, [the default and example configuration for M3DB](https://github.com/m3db/m3/tree/master/src/dbnode/config) favors performance and availability, as that is well-suited for M3DB's most common metrics and Observability use cases. To instead favor consistency and durability, consider tuning values as described in the "Tuning for Consistency and Durability" section.
Database operators who are using M3DB for workloads that require stricter consistency and durability guarantees should consider tuning the default configuration to better suit their use case.

The rest of this document describes the various configuration options that are available to M3DB operators to make such tradeoffs.
While reading it, we recommend referring to [the default configuration file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-all-config.yml) (which has every possible configuration value set) to see how the described values fit into M3DB's configuration as a whole.

## Tuning for Performance and Availability

### Client Write and Read consistency

We recommend running the client with `writeConsistencyLevel` set to `majority` and `readConsistencyLevel` set to `unstrict_majority`.
This means that all write must be acknowledged by a quorums of nodes in order to be considered succesful, and that reads will attempt to achieve quorum, but will return the data from a single node if they are unable to achieve quorum. This ensures that reads will normally ensure consistency, but degraded conditions will cause reads to fail outright as long as at least a single node can satisfy the request.

You can read about the consistency levels in more detail in [the Consistency Levels section](/v1.0/docs/m3db/architecture/consistencylevels)

### Commitlog Configuration

We recommend running M3DB with an asynchronous commitlog.
This means that writes will be reported as successful by the client, though the data may not have been flushed to disk yet.

For example, consider the default configuration:

```yaml
commitlog:
  flushMaxBytes: 524288
  flushEvery: 1s
  queue:
    calculationType: fixed
    size: 2097152
```

This configuration states that the commitlog should be flushed whenever either of the following is true:

1.  524288 or more bytes have been written since the last time M3DB flushed the commitlog.
2.  One or more seconds has elapsed since the last time M3DB flushed the commitlog.

In addition, the configuration also states that M3DB should allow up to `2097152` writes to be buffered in the commitlog queue before the database node will begin rejecting incoming writes so it can attempt to drain the queue and catch up. Increasing the size of this queue can often increase the write throughput of an M3DB node at the cost of potentially losing more data if the node experiences a sudden failure like a hard crash or power loss.

### Writing New Series Asynchronously

The default M3DB YAML configuration will contain the following as a top-level key under the `db` section:

```yaml
writeNewSeriesAsync: true
```

This instructs M3DB to handle writes for new timeseries (for a given time block) asynchronously. Creating a new timeseries in memory is much more expensive than simply appending a new write to an existing series, so the default configuration of creating them asynchronously improves M3DBs write throughput significantly when many new series are being created all at once.

However, since new time series are created asynchronously, it's possible that there may be a brief delay inbetween when a write is acknowledged by the client and when that series becomes available for subsequent reads.

M3DB also allows operators to rate limit the number of new series that can be created per second via the following configuration under the `db.limits` section:

```yaml
db:
  limits:
    writeNewSeriesPerSecond: 1048576
```

This value can be set much lower than the default value for workloads in which a significant increase in cardinality usually indicates a misbehaving caller.

### Ignoring Corrupt Commitlogs on Bootstrap

If M3DB is shut down gracefully (i.e via SIGTERM), it will ensure that all pending writes are flushed to the commitlog on disk before the process exists.
However, in situations where SIGKILL is used, the process exited unexpectedly or the node itself experienced a sudden failure, the tail end of the commitlog may be corrupt.
In such situations, M3DB will read as much of the commitlog as possible in an attempt to recover the maximum amount of data. However, it then needs to make a decision: it can either **(a)** come up successfully and tolerate an ostensibly minor amount of data or loss, or **(b)** attempt to stream the missing data from its peers.
This behavior is controlled by the following default configuration:

```yaml
bootstrap:
  commitlog:
    returnUnfulfilledForCorruptCommitLogFiles: false
```

In the situation where only a single node fails, the optimal outcome is for the node to attempt to repair itself from one of its peers.
However, if a quorum of nodes fail and encounter corrupt commitlog files, they will deadlock while attempting to stream data from each other, as no nodes will be able to make progress due to a lack of quorum.
This issue requires an operator with significant M3DB operational experience to manually bootstrap the cluster; thus the official recommendation is to set `returnUnfulfilledForCorruptCommitLogFiles: false` to avoid this issue altogether. In most cases, a small amount of data loss is preferable to a quorum of nodes that crash and fail to start back up automatically.

## Tuning for Consistency and Durability

### Client Write and Read consistency

The most important thing to understand is that **if you want to guarantee that you will be able to read the result of every successful write, then both writes and reads must be done with `majority` consistency.**
This means that both writes _and_ reads will fail if a quorum of nodes are unavailable for a given shard.
You can read about the consistency levels in more detail in [the Consistency Levels section](/v1.0/docs/m3db/architecture/consistencylevels)

### Commitlog Configuration

M3DB supports running the commitlog synchronously such that every write is flushed to disk and fsync'd before the client receives a successful acknowledgement, but this is not currently exposed to users in the YAML configuration and generally leads to a massive performance degradation.
We only recommend operating M3DB this way for workloads where data consistency and durability is strictly required, and even then there may be better alternatives such as running M3DB with the bootstrapping configuration: `filesystem,peers,uninitialized_topology` as described in our [bootstrapping operational guide](/v1.0/docs/operational_guide/bootstrapping_crash_recovery).

### Writing New Series Asynchronously

If you want to guarantee that M3DB will immediately allow you to read data for writes that have been acknowledged by the client, including the situation where the previous write was for a brand new timeseries, then you  will need to change the default M3DB configuration to set `writeNewSeriesAsync: false` as a top-level key under the `db` section:

```yaml
writeNewSeriesAsync: false
```

This instructs M3DB to handle writes for new timeseries (for a given time block) synchronously. Creating a new timeseries in memory is much more expensive than simply appending a new write to an existing series, so this configuration could have an adverse effect on performance when many new timeseries are being inserted into M3DB concurrently.

Since this operation is so expensive, M3DB allows operator to rate limit the number of new series that can be created per second via the following configuration (also a top-level key under the `db.limits` section):

```yaml
db:
  limits:
    writeNewSeriesPerSecond: 1048576
```

### Ignoring Corrupt Commitlogs on Bootstrap

As described in the "Tuning for Performance and Availability" section, we recommend configuring M3DB to ignore corrupt commitlog files on bootstrap. However, if you want to avoid any amount of inconsistency or data loss, no matter how minor, then you should configure M3DB to return unfulfilled when the commitlog bootstrapper encounters corrupt commitlog files. You can do so by modifying your configuration to look like this:

```yaml
bootstrap:
  commitlog:
    returnUnfulfilledForCorruptCommitLogFiles: true
```

This will force your M3DB nodes to attempt to repair corrupted commitlog files on bootstrap by streaming the data from their peers.
In most situations this will be transparent to the operator and the M3DB node will finish bootstrapping without trouble.
However, in the scenario where a quorum of nodes for a given shard failed in unison, the nodes will deadlock while attempting to stream data from each other, as no nodes will be able to make progress due to a lack of quorum.
This issue requires an operator with significant M3DB operational experience to manually bootstrap the cluster; thus the official recommendation is to avoid configuring M3DB in this way unless data consistency and durability are of utmost importance.
