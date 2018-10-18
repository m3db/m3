# Storage Engine Overview

M3DB is a time series database that was primarily designed to be horizontally scalable and handle a large volume of monitoring time series data.

## Time Series Compression (M3TSZ)

One of M3DB's biggest strengths as a time series database (as opposed to using a more general-purpose horizontally scalable, distributed database like Cassandra) is its ability to compress time series data resulting in huge memory and disk savings. This high compression ratio is implemented via the M3TSZ algorithm, a variant of the streaming time series compression algorithm described in [Facebook's Gorilla paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) with a few small differences.

The compression ratio will vary depending on the workload and configuration, but we found that with M3TSZ we were able to achieve a compression ratio of 1.45 bytes/datapoint with Uber's production workloads. This was a 40% improvement over standard TSZ which only gave us a compression ratio of 2.42 bytes/datapoint under the same conditions.

## Architecture

M3DB is a persistent database with durable storage, but it is best understood via the boundary between its in-memory object layout and on-disk representations.

### In-Memory Object Layout

```
                   ┌───────────────────────────────┐
   ┌───────────────┤           Database            ├─────────────────┐
   │               └───────────────────────────────┘                 │
   │                                                                 │
   │                                                                 │
   │                                                                 │
   │               ┌───────────────────────────────┐                 │
   │     ┌─────────┤          Namespace 1          ├──────────┐      │
   │     │         └───────────────────────────────┘          │      │
   │     │                                                    │      │
   │     │                                                    │      │
   │     │                   ┌───────────┐                    │      │
   │     │    ┌──────────────┤  Shard 1  ├──────────────┐     │      │
   │     │    │              └───────────┘              │     │      │
   │     │    │                                         │     │      │
   │     │    │                                         │     │      │
   │     │    │              ┌───────────┐              │     │      │
   │     │    │   ┌──────────┤ Series 1  ├──────────┐   │     │      │
   │     │    │   │          └───────────┘          │   │     │      │
   │     │    │   │                                 │   │     │      │
   │     │    │   │                                 │   │     │      │
   │     │    │   │ ┌─────────────────────────────┐ │   │     │      │
   │     │    │   │ │      Block [2PM - 4PM]      │ │   │     │      │
   │     │    │   │ ├─────────────────────────────┤ │   │     │      │
   │     │    │   │ │      Block [4PM - 6PM]      │ │   │     │      │
   │     │    │   │ ├─────────────────────────────┤ │   │     │      │
   │     │    │   │ │       ┌────────────┐        │ │   │     │      │
   │     │    │   │ └───────┤   Blocks   ├────────┘ │   │     │      │
   │     │    │   │         └────────────┘          │   │     │      │
   │     │    │   │                                 │   │     │      │
   │     │    │   │                                 │   │     │      │
   │     │    │   │  ┌────────────────────────────┐ │   │     │      │
   │     │    │   │  │                            │ │   │     │      │
   │     │    │   │  │     Block [6PM - 8PM]      │ │   │     │      │
   │     │    │   │  │                            │ │   │     │      │
   │     │    │   │  ├────────────────────────────┤ │   │     │      │
   │     │    │   │  │ Active Buffers (encoders)  │ │   │     │      │
   │     │    │   │  └────────────────────────────┘ │   │     │      │
   │     │    │   │                                 │   │     │      │
   │     │    │   │                                 │   │     │      │
   │     │    │   └─────────────────────────────────┘   │     │      │
   │     │    │                                         │     │      │
   │     │    │                                         │     │      │
   │     │    │                                         │     │      │
   │     │    │                                         │     │      │
   │     │    └─────────────────────────────────────────┘     │      │
   │     │                                                    │      │
   │     │                                                    │      │
   │     └────────────────────────────────────────────────────┘      │
   │                                                                 │
   └─────────────────────────────────────────────────────────────────┘
```

The in-memory portion of M3DB is implemented via a hierarchy of objects:

1. A `database` of which there is only one per M3DB process.

2. A `database` "owns" numerous namespaces, and each namespace has a unique name as well as distinct configuration with regards to data retention and blocksize (which we will discuss in more detail later). `Namespaces` are similar to tables in other databases.

3. [`Shards`](sharding.md) which are owned by `namespaces`. `Shards` are effectively the same as "virtual shards" in Cassandra in that they provide arbitrary distribution of time series data via a simple hash of the series ID.

4. `Series` which are owned by `shards`. A `series` is generally what comes to mind when you think of "time series" data. Ex. The CPU level for a single host in a datacenter over a period of time could be represented as a series with id "<HOST_IDENTIFIER>.system.cpu.utilization" and a vector of tuples in the form of (TIMESTAMP, CPU_LEVEL). In other words, if you were rendering a graph a series would represent a single line on that graph. Note that the previous example is only a logical illustration and does not represent the way that M3DB actually stores data.

5. `Blocks` belong to a series and are central to M3DB's design. A `block` is simply a smaller wrapper object around a sealed (no longer writable) stream of compressed time series data. The compression comes with a few caveats though, namely that you cannot read individual datapoints in a compressed block. In other words, in order to read a single datapoint you must decompress the entire block up to the datapoint that you're trying to read.

If M3DB kept everything in memory (and in fact, early versions of it did), than you could conceptually think of it as being a composed from a hierarchy of maps:

database_obect      => map<namepace_name, namespace_object>
namespace_object    => map<shard_id, shard_object>
shard_object        => map<series_id, series_object>
series_object       => map<block_start_time, block_object>
series_object       => map<block_start_time, active_buffers(encoders)> (This map should only have one or two entries)

### Persistent storage

While in-memory databases can be useful (and M3DB supports operating in a memory-only mode), some form of persistence is required for durability. In other words, without a persistence strategy then it would be impossible for M3DB to restart (or recover from a crash) without losing all of its data.

In addition, with large volumes of data it becomes prohibitively expensive to keep all of the data in memory. This is especially true for monitoring workloads which often follow a "write-once, read-never" pattern where less than a few percent of all the data that's stored is ever read. With that type of workload, its wasteful to keep all of that data in memory when it could be persisted on disk and retrieved when required.

Like most other databases, M3DB takes a two-pronged approach to persistant storage that involves combining a commitlog (for disaster recovery) with periodic snapshotting (for efficient retrieval):

1. All writes are persisted to a [commitlog](commitlogs.md) (the commitlog can be configured to fsync every write, or optionally batch writes together which is much faster but leaves open the possibility of small amounts of data loss in the case of a catastrophic failure). The commitlog is completely uncompressed and exists only to recover "unflushed" data in the case of a database shutdown (intentional or not) and is never used to satisfy a read request.
2. Periodically (based on the configured blocksize) all "active" blocks are "sealed" (marked as immutable) and flushed to disk as ["fileset" files](storage.md). These files are highly compressed and can be indexed into via their complementary index files. Check out the [flushing section](engine.md#flushing) to learn more about the background flushing process.

The blocksize parameter is the most important variable that needs to be tuned for your particular workload. A small blocksize will mean more frequent flushing and a smaller memory footprint for the data that is being actively compressed, but it will also reduce the compression ratio and your data will take up more space on disk.

If the database is stopped for any reason in-between "flushes" (writing fileset files out to disk), then when the node is started back up those writes will need to be recovered by reading the commitlog or streaming in the data from a peer responsible for the same shard (if the replication factor is larger than 1).

While the [fileset files](storage.md) are designed to support efficient data retrieval via the series primary key (the ID), there is still a heavy cost associated with any query that has to retrieve data from disk because going to disk is always much slower than accessing main memory. To compensate for that, M3DB support various [caching policies](caching.md) which can significantly improve the performance of reads by caching data in memory.

## Write Path

We now have enough context of M3DB's architecture to discuss the lifecycle of a write. A write begins when an M3DB client calls the [`writeBatchRaw`](https://github.com/m3db/m3/blob/06d3ecc94d13cff67b82a791271816caa338dcab/src/dbnode/generated/thrift/rpc.thrift#L59) endpoint on M3DB's embedded thrift server. The write itself will contain the following information:

1. The namespace
2. The series ID (byte blob)
3. The timestamp
4. The value itself

M3DB will consult the database object to check if the namespace exists, and if it does,then it will hash the series ID to determine which shard it belongs to. If the node receiving the write owns that shard, then it will lookup the series in the shard object. If the series exists, then it will lookup the series' corresponding encoder and encode the datapoint into the compressed stream. If the encoder doesn't exist (no writes for this series have occurred yet as part of this block) then a new encoder will be allocated and it will begin a compressed M3TSZ stream with that datapoint. There is also some special logic for handling out-of-order writes which is discussed in the [merging all encoders section](engine.md#merging-all-enoders).

At the same time, the write will be appended to the commitlog queue (and depending on the commitlog configuration immediately fsync'd to disk or batched together with other writes and flushed out all at once).

The write will exist only in this "active buffer" and the commitlog until the block ends and is flushed to disk, at which point the write will exist in a fileset file for efficient storage and retrieval later and the commitlog entry can be garbage collected.

**Note:** Regardless of the success or failure of the write in a single node, the client will return a success or failure to the caller for the write based on the configured [consistency level](consistencylevels.md).

## Read Path

A read begins when an M3DB client calls the [`FetchBatchResult`](https://github.com/m3db/m3/blob/master/generated/thrift/rpc.thrift) or [`FetchBlocksRawResult`](https://github.com/m3db/m3/blob/master/generated/thrift/rpc.thrift) endpoint on M3DB's embedded thrift server. The read request will contain the following information:

1. The namespace
2. The series ID (byte blob)
3. The period of time being requested (start and end)

M3DB will consult the database object to check if the namespace exists, and if it does, then it will hash the series ID to determine which shard it belongs to. If the node receiving the read owns that shard, then M3DB needs to determine two things:

1. Does the series exist? and if it does
2. Does the data exist in an "active buffer" (actively being compressed by an encoder), cached in-memory, on disk, or some combination of all three?

Determining whether the series exists is simple. M3DB looks up the series in the shard object. If it exists, then the series exists. If it doesn't, then M3DB consults an in-memory bloom filter(s) for that shard / block start combination(s) to determine if the series exists on disk.

If the series exists, then for every block that the request spans, M3DB needs to consolidate data from the active buffers, in-memory cache, and fileset files (disk).

Lets imagine a read for a given series that requests the last 6 hours worth of data, and an M3DB namespace that is configured with a blocksize of 2 hours (i.e we need to find 3 different blocks.)

If the current time is 8PM, then the location of the requested blocks might be as follows:

```
[2PM - 4PM (FileSet file)]    - Sealed and flushed block that isn't cached
[4PM - 6PM (In-memory cache)] - Sealed and flush block that is cached
[6PM - 8PM (active buffer)]   - Hasn't been sealed or flushed yet
```

Then M3DB will need to consolidate:

1. The not-yet-sealed block from the active buffers / encoders (located inside an internal lookup in the Series object) **[6PM - 8PM]**
2. The in-memory cached block (also located inside an internal lookup in the Series object) **[4PM - 6PM]**
3. The block from disk (the block retrieve from disk will then be cached according to the current [caching policy](caching.md) **[2PM - 4PM]**

Retrieving blocks from the active buffers and in-memory cache is simple, the data is already present in memory and easily accessible via hashmaps keyed by series ID. Retrieving a block from disk is more complicated. The flow for retrieving a block from disk is as follows:

1. Consult the in-memory bloom filter to determine if its possible the series exists on disk.
2. If the bloom filter returns positive, then binary search the in-memory index summaries to find the nearest index entry that is *before* the series ID that we're searching for. Review the `index_lookup.go` file for implementation details.
3. Jump to the offset in the index file that we obtained from the binary search in the previous step, and begin scanning forward until we identify the index entry for the series ID we're looking for *or* we get far enough in the index file that it becomes clear that the ID we're looking for doesn't exist (this is possible because the index file is sorted by ID)
4. Jump to the offset in the data file that we obtained from scanning the index file in the previous step, and begin streaming data.

Once M3DB has retrieved the three blocks from their respective locations in memory / on-disk, it will transmit all of the data back to the client. Whether or not the client returns a success to the caller for the read is dependent on the configured [consistency level](consistencylevels.md).

**Note:** Since M3DB nodes return compressed blocks (the M3DB client decompresses them), it's not possible to return "partial results" for a given block. If any portion of a read request spans a given block, then that block in its entirety must be transmitted back to the client. In practice, this ends up being not much of an issue because of the high compression ratio that M3DB is able to achieve.

## Background processes

M3DB has a variety of processes that run in the background during normal operation.

### Ticking

The ticking process runs continously in the background and is responsible for a variety of tasks:

1. Merging all encoders for a given series / block start combination
2. Removing expired / flushed series and blocks from memory
3. Cleanup of expired data (fileset/commit log) from the filesystem


#### Merging all encoders

M3TSZ is designed for compressing time series data in which each datapoint has a timestamp that is larger than the last encoded datapoint. For monitoring workloads this works very well because every subsequent datapoint is almost always larger than the previous one. However, real world systems are messy and occasionally out of order writes will be received. When this happens, M3DB will allocate a new encoder for the out of order datapoints. The multiple encoders need to be merged before flushing the data to disk, but to prevent huge memory spikes during the flushing process we continuously merge out of order encoders in the background.

#### Removing expired / flushed series and blocks from memory

Depending on the configured [caching policy](caching.md), the [in-memory object layout](engine.md#in-memory-object-layout) can end up with references to series or data blocks that are expired (have fallen out of the retention period) or no longer need to be in memory (due to the data being flushed to disk or no longer needing to be cached). The background tick will identify these structures and release them from memory.

### Flushing

As discussed in the [architecture](engine.md#architecture) section, writes are actively buffered / compressed in-memory and the commit log is continuously being written to, but eventually data needs to be flushed to disk in the form of [fileset files](storage.md) to facilitate efficient storage and retrieval.

This is where the configurable "blocksize" comes into play. The blocksize is simply a duration of time that dictates how long active writes will be compressed (in a streaming manner) in memory before being "sealed" (marked as immutable) and flushed to disk. Lets use a blocksize of two hours as an example.

If the blocksize is set to two hours, then all writes for all series for a given shard will be buffered in memory for two hours at a time. At the end of the two hour period all of the [fileset files](storage.md) will be generated, written to disk, and then the in-memory objects can be released and replaced with new ones for the new block. The old objects will be removed from memory in the subsequent tick.

## Caveats / Limitations

1. M3DB currently supports exact ID based lookups. It does not support tag/secondary indexing. This feature is under development and future versions of M3DB will have support for a built-in reverse index.
2. M3DB does not support updates / deletes. All data written to M3DB is immutable.
3. M3DB does not support writing arbitrarily into the past and future. This is generally fine for monitoring workloads, but can be problematic for traditional [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) and [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) workloads. Future versions of M3DB will have better support for writes with arbitrary timestamps.
4. M3DB does not support writing datapoints with values other than double-precision floats. Future versions of M3DB will have support for storing arbitrary values.
5. M3DB does not support storing data with an indefinite retention period, every namespace in M3DB is required to have a retention policy which specifies how long data in that namespace will be retained for. While there is no upper bound on that value (Uber has production databases running with retention periods as high as 5 years), its still required and generally speaking M3DB is optimized for workloads with a well-defined [TTL](https://en.wikipedia.org/wiki/Time_to_live).
6. M3DB does not support either background data repair or Cassandra-style [read repairs](https://docs.datastax.com/en/cassandra/2.1/cassandra/operations/opsRepairNodesReadRepair.html). Future versions of M3DB will support automatic repairs of data as an ongoing background process.