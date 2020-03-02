---
title: "M3DB node management"
date: 2020-04-21T20:58:41-04:00
draft: true
---

#### Write Path
We now have enough context of M3DB's architecture to discuss the lifecycle of a write. A write begins when an M3DB client calls the writeBatchRaw endpoint on M3DB's embedded thrift server. The write itself will contain the following information:

- The namespace
- The series ID (byte blob)
- The timestamp
- The value itself

M3DB will consult the database object to check if the namespace exists, and if it does, then it will hash the series ID to determine which shard it belongs to. If the node receiving the write owns that shard, then it will lookup the series in the shard object. If the series exists, then an encoder in the buffer will encode the datapoint into the compressed stream. If the encoder doesn't exist (no writes for this series have occurred yet as part of this block) then a new encoder will be allocated and it will begin a compressed M3TSZ stream with that datapoint. There is also some additional logic for handling multiple encoders and filesets which is discussed in the buffer section.

At the same time, the write will be appended to the commit log, which is periodically compacted via a snapshot process. Details of this is outlined in the commit log page.

Note: Regardless of the success or failure of the write in a single node, the client will return a success or failure to the caller for the write based on the configured consistency level.

#### Read Path
A read begins when an M3DB client calls the FetchBatchResult or FetchBlocksRawResult endpoint on M3DB's embedded thrift server. The read request will contain the following information:
- The namespace
- The series ID (byte blob)
- The period of time being requested (start and end)

M3DB will consult the database object to check if the namespace exists, and if it does, it will hash the series ID to determine which shard it belongs to. If the node receiving the read owns that shard, then 

M3DB needs to determine two things:
- Whether the series exists and if it does,
- Whether the data exists in the buffer, cached in-memory, on disk, or some combination of all three.

Determining whether the series exists is simple. M3DB looks up the series in the shard object. If it exists, then the series exists. If it doesn't, then M3DB consults in-memory bloom filters(s) for all shard/block start combinations(s) that overlap the query range to determine if the series exists on disk.

If the series exists, then for every block that the request spans, M3DB needs to consolidate data from the buffer, in-memory cache, and fileset files (disk).

Let's imagine a read for a given series that requests the last 6 hours worth of data, and an M3DB namespace that is configured with a block size of 2 hours, i.e. we need to find 3 different blocks.

If the current time is 8PM, then the location of the requested blocks might be as follows:
[2PM - 4PM (fileset file)]                - Flushed block that isn't cached
[4PM - 6PM (in-memory cache)]             - Flushed block that is cached
[4PM - 6PM (cold write in active buffer)] - Cold write that hasn't been flushed yet
[6PM - 8PM (active buffer)]               - Hasn't been flushed yet

Then M3DB will need to consolidate:
- The not-yet-sealed block from the buffer (located inside an internal lookup in the Series object) [6PM - 8PM]
- The in-memory cached block (also located inside an internal lookup in the Series object). Since there are also cold writes in this block, the cold writes will be consolidated in memory with data found in the cached block before returning. [4PM - 6PM]
- The block from disk (the block will be retrieved from disk and will then be cached according to the current caching policy) [2PM - 4PM]
- Retrieving blocks from the buffer and in-memory cache is simple, the data is already present in memory and easily accessible via hashmaps keyed by series ID. Retrieving a block from disk is more complicated. - The flow for retrieving a block from disk is as follows:
- Consult the in-memory bloom filter to determine if it's possible the series exists on disk.
- If the bloom filter returns negative, we are sure that the series isn't there, so return that result. If the bloom filter returns positive, then binary search the in-memory index summaries to find the nearest index entry that is before the series ID that we're searching for. Review the index_lookup.go file for implementation details.
- Jump to the offset in the index file that we obtained from the binary search in the previous step, and begin scanning forward until we identify the index entry for the series ID we're looking for or we get far enough in the index file that it becomes clear that the ID we're looking for doesn't exist (this is possible because the index file is sorted by ID)
- Jump to the offset in the data file that we obtained from scanning the index file in the previous step, and begin streaming data.
- Once M3DB has retrieved the three blocks from their respective locations in memory / on-disk, it will transmit all of the data back to the client. Whether or not the client returns a success to the caller for the read is dependent on the configured consistency level.

Note: Since M3DB nodes return compressed blocks (the M3DB client decompresses them), it's not possible to return "partial results" for a given block. If any portion of a read request spans a given block, then that block in its entirety must be transmitted back to the client. In practice, this ends up being not much of an issue because of the high compression ratio that M3DB is able to achieve.

#### Buffer
Each series object contains a buffer, which is in charge of handling all data that has yet to be flushed - new writes and bootstrapped data. To accomplish this, it keeps mutable "buckets" of encoders (for new writes) and immutable blocks (for bootstrapped data). M3TSZ, the database's encoding scheme, is designed for compressing time series data in which each datapoint has a timestamp that is larger than the last encoded datapoint. For metrics workloads this works very well because every subsequent datapoint is almost always after the previous one. However, out of order writes will occasionally be received, for example due to clock skew. When this happens, M3DB will allocate a new encoder for the out of order datapoints. These encoders are contained in a bucket along with any blocks that got bootstrapped.

Upon a flush (discussed further below), all data within a bucket gets merged and its version gets incremented - the specific version it gets set to depends on the number of times this block has previously been flushed. This bucket versioning allows the buffer to know which data has been flushed so that subsequent flushes will not try to flush it again. It also indicates to the clean up process (also discussed below) that that data can be evicted.

Given this complex, concurrent logic, this has been modeled in TLA.
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

### Background processes
M3DB has a variety of processes that run in the background during normal operation.
#### Flushing
As discussed in the architecture section, writes are actively buffered / compressed in memory and the commit log is continuously being written to, but eventually data needs to be flushed to disk in the form of fileset files to facilitate efficient storage and retrieval.
This is where the configurable "block size" comes into play. The block size is simply a duration of time that dictates how long new writes will be compressed (in a streaming manner) in memory before being flushed to disk. Let's use a block size of two hours as an example.
If the block size is set to two hours, then all writes for all series for a given shard will be buffered in memory for two hours at a time. At the end of the two hour period all of the fileset files will be generated, written to disk, and then the in-memory objects can be released and replaced with new ones for the new block. The old objects will be removed from memory in the subsequent tick.
If a flush happens for a namespace/shard/series/block for which there is already a fileset, in-memory data will get merged with data on disk from the fileset. The resultant merged data will then be flushed as a separate fileset.

#### Ticking
The ticking process runs continously in the background and is responsible for a variety of tasks:
Merging all encoders for a given series / block start combination
Removing expired / flushed series and blocks from memory
Clean up of expired data (fileset/commit log) from the filesystem

#### Merging all encoders
If there are multiple encoders for a block, they need to be merged before flushing the data to disk. To prevent huge memory spikes during the flushing process we continuously merge out of order encoders in the background.

#### Removing expired / flushed series and blocks from memory
Depending on the configured caching policy, the in-memory object layout can end up with references to series or data blocks that are expired (have fallen out of the retention period) or no longer needed to be in memory (due to the data being flushed to disk or no longer needing to be cached). The background tick will identify these structures and release them from memory.

#### Clean up of expired data
Fileset files can become no longer necessary for two reasons:
The fileset files for a block that has fallen out of retention
A flush occurred for a block that already has a fileset file. The new fileset will be a superset of the existing fileset with any new data that for that block, hence, the existing fileset is no longer required
During the clean up process, these fileset files will get deleted.

#### Caveats / Limitations
Currently M3DB does not support deletes.
M3DB does not support storing data with an indefinite retention period, every namespace in M3DB is required to have a retention policy which specifies how long data in that namespace will be retained for. While there is no upper bound on that value, it's still required and generally speaking M3DB is optimized for workloads with a well-defined TTL.
M3DB does not support either background data repair or Cassandra-style read repairs. Future versions of M3DB will support automatic repairs of data as an ongoing background process.
M3DB does not support writing far into the future. Support for this will be added in future


### Cluster operations
#### Node add
When a node is added to the cluster it is assigned shards that relieves load fairly from the existing nodes. The shards assigned to the new node will become INITIALIZING, the nodes then discover they need to be bootstrapped and will begin bootstrapping the data using all replicas available. The shards that will be removed from the existing nodes are marked as LEAVING.

#### Node down
A node needs to be explicitly taken out of the cluster. If a node goes down and is unavailable the clients performing reads will be served an error from the replica for the shard range that the node owns. During this time it will rely on reads from other replicas to continue uninterrupted operation.

#### Node remove
When a node is removed the shards it owns are assigned to existing nodes in the cluster. Remaining servers discover they are now in possession of shards that are INITIALIZING and need to be bootstrapped and will begin bootstrapping the data using all replicas available.

