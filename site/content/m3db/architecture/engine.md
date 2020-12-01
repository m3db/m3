---
title: "Storage Engine"
weight: 1
---

M3DB is a time series database that was primarily designed to be horizontally scalable and able to handle high data throughput.

## Time Series Compression

One of M3DB's biggest strengths as a time series database (as opposed to using a more general-purpose horizontally scalable, distributed database like Cassandra) is its ability to compress time series data resulting in huge memory and disk savings. There are two compression algorithms used in M3DB: M3TSZ and protobuf encoding.

### M3TSZ

M3TSZ is used when values are floats. A variant of the streaming time series compression algorithm described in [Facebook's Gorilla paper](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf), it achieves a high compression ratio. The compression ratio will vary depending on the workload and configuration, but we found that we were able to achieve a compression ratio of 1.45 bytes/datapoint with Uber's production workloads. This was a 40% improvement over standard TSZ, which only gave us a compression ratio of 2.42 bytes/datapoint under the same conditions.

### Protobuf Encoding

For more complex value types, M3DB also supports generic Protobuf messages with [a few exceptions](https://github.com/m3db/m3/blob/master/src/dbnode/encoding/proto/docs/encoding.md#supported-syntax). The algorithm takes on a hybrid approach and uses different compression schemes depending on the field types within the Protobuf message.

Details on the encoding, marshaling and unmarshaling methods can be read [here](https://github.com/m3db/m3/tree/master/src/dbnode/encoding/proto).

## Architecture

M3DB is a persistent database with durable storage, but it is best understood via the boundary between its in-memory object layout and on-disk representations.

### In-Memory Object Layout

       ┌────────────────────────────────────────────────────────────┐
       │                          Database                          │
       ├────────────────────────────────────────────────────────────┤
       │                                                            │
       │   ┌────────────────────────────────────────────────────┐   │
       │   │                     Namespaces                     │   │
       │   ├────────────────────────────────────────────────────┤   │
       │   │                                                    │   │
       │   │   ┌────────────────────────────────────────────┐   │   │
       │   │   │                   Shards                   │   │   │
       │   │   ├────────────────────────────────────────────┤   │   │
       │   │   │                                            │   │   │
       │   │   │   ┌────────────────────────────────────┐   │   │   │
       │   │   │   │               Series               │   │   │   │
       │   │   │   ├────────────────────────────────────┤   │   │   │
       │   │   │   │                                    │   │   │   │
       │   │   │   │   ┌────────────────────────────┐   │   │   │   │
       │   │   │   │   │           Buffer           │   │   │   │   │
       │   │   │   │   └────────────────────────────┘   │   │   │   │
       │   │   │   │                                    │   │   │   │
       │   │   │   │                                    │   │   │   │
       │   │   │   │   ┌────────────────────────────┐   │   │   │   │
       │   │   │   │   │       Cached blocks        │   │   │   │   │
       │   │   │   │   └────────────────────────────┘   │   │   │   │
       │   │   │   │                ...                 │   │   │   │
       │   │   │   │                                    │   │   │   │
       │   │   │   └────────────────────────────────────┘   │   │   │
       │   │   │                    ...                     │   │   │
       │   │   │                                            │   │   │
       │   │   └────────────────────────────────────────────┘   │   │
       │   │                        ...                         │   │
       │   │                                                    │   │
       │   └────────────────────────────────────────────────────┘   │
       │                            ...                             │
       │                                                            │
       └────────────────────────────────────────────────────────────┘

The in-memory portion of M3DB is implemented via a hierarchy of objects:

1.  A `database` of which there is only one per M3DB process. The `database` owns multiple `namespace`s.
2.  A `namespace` is similar to a table in other databases. Each `namespace` has a unique name and a set of configuration options, such as data retention and block size (which we will discuss in more detail later). A namespace owns multiple `shard`s.
3.  A [`shard`](/docs/m3db/architecture/sharding) is effectively the same as a "virtual shard" in Cassandra in that it provides an arbitrary distribution of time series data via a simple hash of the series ID. A shard owns multiple `series`.
4. A `series` represents a sequence of time series datapoints. For example, the CPU utilization for a host could be represented as a series with the ID "host1.system.cpu.utilization" (or "__name__=system_cpu_utilization,host=host1"), and a vector of (TIMESTAMP, CPU_LEVEL) tuples. Visualizing this example in a graph, there would a single line with time on the x-axis and CPU utilization on the y-axis. A `series` owns a `buffer` and any cached `block`s.
5. The `buffer` is where all data that has yet to be written to disk gets stored in memory, for a specific series. This includes both new writes to M3DB and data obtained through [bootstrapping](/docs/operational_guide/bootstrapping_crash_recovery). More details on the [buffer](/docs/m3db/architecture/engine#buffer) is explained below. Upon [flushing](/docs/m3db/architecture/engine#flushing), the buffer creates a `block` of its data to be persisted to disk.
6. A `block` represents a stream of compressed single time series data for a pre-configured block size, for example, a block could hold data for 6-8PM (block size of two hours). A `block` can arrive directly into the series only as a result of getting cached after a read request. Since blocks are in a compressed format, individual datapoints cannot be read from it. In other words, in order to read a single datapoint, the entire block up to that datapoint needs to be decompressed beforehand.

### Persistent storage

While in-memory databases can be useful (and M3DB supports operating in a memory-only mode), some form of persistence is required for durability. In other words, without a persistence strategy, it would be impossible for M3DB to restart (or recover from a crash) without losing all of its data.

In addition, with large volumes of data, it becomes prohibitively expensive to keep all the data in memory. This is especially true for monitoring workloads which often follow a "write-once, read-never" pattern where less than a few percent of all the data that's stored is ever read. With that type of workload, it's wasteful to keep all of that data in memory when it could be persisted on disk and retrieved when required.

M3DB takes a two-pronged approach to persistent storage that involves combining a [commit log](/docs/m3db/architecture/commitlogs) for disaster recovery with periodic flushing (writing [fileset files](/docs/m3db/architecture/storage) to disk) for efficient retrieval:

1.  All writes are persisted to a commit log (the commit log can be configured to fsync every write, or optionally batch writes together which is much faster but leaves open the possibility of small amounts of data loss in the case of a catastrophic failure). The commit log is completely uncompressed and exists only to recover unflushed data in the case of a database shutdown (intentional or not) and is never used to satisfy a read request.
2.  Periodically (based on the configured block size), all data in the buffer is flushed to disk as immutable [fileset files](/docs/m3db/architecture/storage). These files are highly compressed and can be indexed into via their complementary index files. Check out the [flushing section](#flushing) to learn more about the background flushing process.

The block size parameter is the most important variable that needs to be tuned for a particular workload. A small block size will mean more frequent flushing and a smaller memory footprint for the data that is being actively compressed, but it will also reduce the compression ratio and data will take up more space on disk.

If the database is stopped for any reason in between flushes, then when the node is started back up those writes will be recovered by reading the commit log or streaming in the data from a peer responsible for the same shard (if the replication factor is larger than one).

While the fileset files are designed to support efficient data retrieval via the series ID, there is still a heavy cost associated with any query that has to retrieve data from disk because going to disk is always much slower than accessing main memory. To compensate for that, M3DB supports various [caching policies](/docs/m3db/architecture/caching) which can significantly improve the performance of reads by caching data in memory.

## Write Path

We now have enough context of M3DB's architecture to discuss the lifecycle of a write. A write begins when an M3DB client calls the [`writeBatchRaw`](https://github.com/m3db/m3/blob/06d3ecc94d13cff67b82a791271816caa338dcab/src/dbnode/generated/thrift/rpc.thrift#L59) endpoint on M3DB's embedded thrift server. The write itself will contain the following information:

1.  The namespace
2.  The series ID (byte blob)
3.  The timestamp
4.  The value itself

M3DB will consult the database object to check if the namespace exists, and if it does, then it will hash the series ID to determine which shard it belongs to. If the node receiving the write owns that shard, then it will lookup the series in the shard object. If the series exists, then an encoder in the buffer will encode the datapoint into the compressed stream. If the encoder doesn't exist (no writes for this series have occurred yet as part of this block) then a new encoder will be allocated and it will begin a compressed M3TSZ stream with that datapoint. There is also some additional logic for handling multiple encoders and filesets which is discussed in the [buffer](#buffer) section.

At the same time, the write will be appended to the commit log, which is periodically compacted via a snapshot process. Details of this is outlined in the [commit log](/docs/m3db/architecture/commitlogs) page.

{{% notice warning %}}
Regardless of the success or failure of the write in a single node, the client will return a success or failure to the caller for the write based on the configured [consistency level](/docs/m3db/architecture/consistencylevels).
{{% /notice %}}

{{% notice warning %}}
M3DB "default" write is writeTagged which also accepts list of pairs (tag name, tag value), which it then uses to update the reverse index of the namespace. Work to document that is TBD.
{{% /notice %}}

## Read Path

A read begins when an M3DB client calls the [`FetchBatchResult`](https://github.com/m3db/m3/blob/master/src/dbnode/generated/thrift/rpc.thrift) or [`FetchBlocksRawResult`](https://github.com/m3db/m3/blob/master/src/dbnode/generated/thrift/rpc.thrift) endpoint on M3DB's embedded thrift server. The read request will contain the following information:

1.  The namespace
2.  The series ID (byte blob)
3.  The period of time being requested (start and end)

M3DB will consult the database object to check if the namespace exists, and if it does, it will hash the series ID to determine which shard it belongs to. If the node receiving the read owns that shard, then M3DB needs to determine two things:

1.  Whether the series exists and if it does,
2.  Whether the data exists in the buffer, cached in-memory, on disk, or some combination of all three.

Determining whether the series exists is simple. M3DB looks up the series in the shard object. If it exists, then the series exists. If it doesn't, then M3DB consults in-memory bloom filters(s) for all shard/block start combinations(s) that overlap the query range to determine if the series exists on disk.

If the series exists, then for every block that the request spans, M3DB needs to consolidate data from the buffer, in-memory cache, and fileset files (disk).

Let's imagine a read for a given series that requests the last 6 hours worth of data, and an M3DB namespace that is configured with a block size of 2 hours, i.e. we need to find 3 different blocks.

If the current time is 8PM, then the location of the requested blocks might be as follows:

    [2PM - 4PM (fileset file)]                - Flushed block that isn't cached
    [4PM - 6PM (in-memory cache)]             - Flushed block that is cached
    [4PM - 6PM (cold write in active buffer)] - Cold write that hasn't been flushed yet
    [6PM - 8PM (active buffer)]               - Hasn't been flushed yet

Then M3DB will need to consolidate:

1.  The not-yet-sealed block from the buffer (located inside an internal lookup in the Series object) **[6PM - 8PM]**
2.  The in-memory cached block (also located inside an internal lookup in the Series object). Since there are also cold writes in this block, the cold writes will be consolidated in memory with data found in the cached block before returning. **[4PM - 6PM]**
3.  The block from disk (the block will be retrieved from disk and will then be cached according to the current [caching policy](/docs/m3db/architecture/caching)) **[2PM - 4PM]**

Retrieving blocks from the buffer and in-memory cache is simple, the data is already present in memory and easily accessible via hashmaps keyed by series ID. Retrieving a block from disk is more complicated. The flow for retrieving a block from disk is as follows:

1.  Consult the in-memory bloom filter to determine if it's possible the series exists on disk.
2.  If the bloom filter returns negative, we are sure that the series isn't there, so return that result. If the bloom filter returns positive, then binary search the in-memory index summaries to find the nearest index entry that is _before_ the series ID that we're searching for. Review the `index_lookup.go` file for implementation details.
3.  Jump to the offset in the index file that we obtained from the binary search in the previous step, and begin scanning forward until we identify the index entry for the series ID we're looking for _or_ we get far enough in the index file that it becomes clear that the ID we're looking for doesn't exist (this is possible because the index file is sorted by ID)
4.  Jump to the offset in the data file that we obtained from scanning the index file in the previous step, and begin streaming data.

Once M3DB has retrieved the three blocks from their respective locations in memory / on-disk, it will transmit all of the data back to the client. Whether or not the client returns a success to the caller for the read is dependent on the configured [consistency level](/docs/m3db/architecture/consistencylevels).

**Note:** Since M3DB nodes return compressed blocks (the M3DB client decompresses them), it's not possible to return "partial results" for a given block. If any portion of a read request spans a given block, then that block in its entirety must be transmitted back to the client. In practice, this ends up being not much of an issue because of the high compression ratio that M3DB is able to achieve.

## Buffer

Each series object contains a buffer, which is in charge of handling all data that has yet to be flushed - new writes and bootstrapped data (used to initialize a shard). To accomplish this, it keeps a list of "buckets" per block start-time, which contains a list of mutable encoders (for new writes) and immutable blocks (for bootstrapped data). M3TSZ, the database's encoding scheme, is designed for compressing time series data in which each datapoint has a timestamp that is larger than the last encoded datapoint. For metrics workloads this works very well because every subsequent datapoint is almost always after the previous one. However, out of order writes will occasionally be received, for example due to clock skew. When this happens, M3DB will allocate a new encoder for the out of order datapoints. These encoders are contained in a bucket along with any blocks that got bootstrapped.

When datapoint is writen to a buffer, a list of buckets is selected based on the block start-time the timestamp of the datapoint belongs to (e.g. 16:23:20 belongs to the 16:00 block). In that list, only one bucket is the active bucket (version = 0) and it will be written into an encoder in that bucket.

Supporting out-of-order writes entailed defining a time window, ending at now, called the Buffer Past: (now - bufferPast, now). It's a namespace configuration option, which dictates the way this datapoint will get flushed to disk: Warm or Cold. A write of a datapoint which its timestamp is inside the Buffer Past time window is classified as a Warm Write, while before that is a Cold Write. That classification is named Write Type. For every Write Type there exists a single active bucket, which contains the mutable encoders, where new writes for that block start-time are written to.

Upon a flush (discussed further below), all data within a bucket gets merged and its version gets incremented - the new version depends on the number of times this block has previously been flushed. This bucket versioning allows the buffer to know which data has been flushed so that subsequent flushes will not try to flush it again. It also indicates to the cleanup process (also discussed below) that that data can be evicted.

Given this complex, concurrent logic, this has been [modeled in TLA](https://github.com/m3db/m3/blob/master/specs/dbnode/flush/FlushVersion.tla).

               ┌─────────────────────────┐
               │          Buffer         │
               ├─────────────────────────┤
               │                         │
               │   ┌─────────────────┐   │
               │   │  2-4PM buckets  │   │
               │   └─────────────────┘   │
               │                         │
               │   ┌─────────────────┐   │
          ┌────│───│  4-6PM buckets  │   |
          │    │   └─────────────────┘   │
          │    │                         │
          │    │           ...           │
          │    └─────────────────────────┘
          │
          │
          v                               After flush:
       ┌─────────────────────┐            ┌─────────────────────┐
       │    4-6PM buckets    │            │    4-6PM buckets    │
       ├─────────────────────┤            ├─────────────────────┤
       │                     │            │                     │
       │   ┌─────────────┐   │            │   ┌─────────────┐   │
       │   │  Bucket v0  │<--│--writes    │   │  Bucket v3  │   │
       │   └─────────────┘   │            │   └─────────────┘   │
       │                     │            │                     │
       │                     │            │                     │
       │                     │            │                     │
       │                     │            │                     │
       │                     │            │                     │
       └─────────────────────┘            └─────────────────────┘


       More writes after flush:           After clean up:
       ┌─────────────────────┐            ┌─────────────────────┐
       │    4-6PM buckets    │            │    4-6PM buckets    │
       ├─────────────────────┤            ├─────────────────────┤
       │                     │            │                     │
       │   ┌─────────────┐   │            │   ┌─────────────┐   │
       │   │  Bucket v3  │   │            │   │  Bucket v0  │<--│--writes
       │   └─────────────┘   │            │   └─────────────┘   │
       │                     │            │                     │
       │   ┌─────────────┐   │            │                     │
       │   │  Bucket v0  │<--│--writes    │                     │
       │   └─────────────┘   │            │                     │
       │                     │            │                     │
       └─────────────────────┘            └─────────────────────┘

## Background processes

M3DB has a variety of processes that run in the background during normal operation.

### Flushing

As discussed in the [architecture](#architecture) section, writes are actively buffered / compressed in memory and the commit log is continuously being written to, but eventually data needs to be flushed to disk in the form of [fileset files](/docs/m3db/architecture/storage) to facilitate efficient storage and retrieval.

This is where the configurable "block size" comes into play. The block size is simply a duration of time dictating how long new writes will be compressed (in a streaming manner) in memory before being flushed to disk. Let's use a block size of two hours as an example.

If the block size is set to two hours, then all writes for all series for a given shard will be buffered in memory for two hours at a time. At the end of the two hour period all of the fileset files will be generated, written to disk, and then the in-memory objects can be released and replaced with new ones for the new block. The old objects will be removed from memory in the subsequent tick.

If a flush happens for a namespace/shard/series/block for which there is already a fileset, in-memory data will get merged with data on disk from the fileset. The resultant merged data will then be flushed as a separate fileset.

There are two types of flushes: Warm and Cold. Warm is the first flush that will happen to a shard in a given block start-time, thus it's the one generating a file set from the given buffers of all series in a shard. A cold flush will only happen after a warm flush has been executed (i.e. a file-set exists on disk). A warm flush writes all un-flushed Warm Write type buckets, which a Cold Flush merges all un-flushed Cold Write type buckets with existing file-set and creates a new one instead. 

A Cold Flush occurs when ever there exists Cold Write buckets in memory, and this check is runs every Tick interval (defined below)    

### Ticking

The ticking process runs continuously in the background and is responsible for a variety of tasks:

1.  Merging all encoders for a given series / block start combination
2.  Removing expired / flushed series and blocks from memory
3.  Clean up of expired data (fileset/commit log) from the filesystem

#### Merging all encoders

If there are multiple encoders for a block, they need to be merged before flushing the data to disk. To prevent huge memory spikes during the flushing process we continuously merge out of order encoders in the background.

#### Removing expired / flushed series and blocks from memory

Depending on the configured [caching policy](/docs/m3db/architecture/caching), the [in-memory object layout](#in-memory-object-layout) can end up with references to series or data blocks that are expired (have fallen out of the retention period) or no longer needed to be in memory (due to the data being flushed to disk or no longer needing to be cached). The background tick will identify these structures and release them from memory.

#### Clean up of expired data

Fileset files can become no longer necessary for two reasons:

1.  The fileset files for a block that has fallen out of retention
2. A flush occurred for a block that already has a fileset file. The new fileset will be a superset of the existing fileset with any new data that for that block, hence, the existing fileset is no longer required.

During the cleanup process, these fileset files will get deleted.

## Caveats / Limitations

1.  Currently M3DB does not support deletes.
2.  M3DB does not support storing data with an indefinite retention period, every namespace in M3DB is required to have a retention policy which specifies how long data in that namespace will be retained for. While there is no upper bound on that value, it's still required and generally speaking M3DB is optimized for workloads with a well-defined [TTL](https://en.wikipedia.org/wiki/Time_to_live).
3.  M3DB does not support either background data repair or Cassandra-style [read repairs](https://docs.datastax.com/en/cassandra/2.1/cassandra/operations/opsRepairNodesReadRepair.html). Future versions of M3DB will support automatic repairs of data as an ongoing background process.
4. M3DB does not support writing far into the future. Support for this will be added in the future.
