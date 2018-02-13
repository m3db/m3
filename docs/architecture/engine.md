1) TODO: We don't have an index
2) TODO: We don't support deletes, updates
3) TODO: We don't support writing into the future or the past
4) TODO: Caching strategies
5) TODO: Explain fileset files per shard/block start and no compaction
6) TODO: Explain commitlog is never used to satisfy a read, just used for data recovery
7) TODO: Explain data retention

# Overview

M3DB is a Timeseries Database that was primarily designed to be horizontally scalable and handle a large volume of monitoring time series data.

## Architecture

M3DB is a persistent database with durable storage, but it is best understood via the boundary between its in-memory datastructures and on-disk representations.

### In-Memory Datastructures

The in-memory portion of M3DB is implemented via a hierarchy of datastructures:

1. A "database" (one per M3DB node, effectively a singleton)
2. "Namespaces" which are similar to tables or namespaces in other databases. A database "owns" numerous namespaces, and each namespace has a unique name as well as distinct configuration with regards to retention and blocksize (which we will discuss in more detail later)
3. "Shards" (TODO: Link to shards documentation) which are owned by namespaces. Shards are effectively the same as "virtual shards" in Cassandra in that they provide arbitrary distribution of timeseries data via a simple hash of the series ID. Shards are useful through the entire M3DB stack in that they make horizontal scaling and adding / removing nodes without downtime trivial at the cluster level, as well as providing more fine grained lock granularity at the memory level, and finally they inform the filesystem organization in that data belonging to the same shard will be used / dropped together and can be kept in the smae file.
4. "Series" which are owned by shards. A series is the minimum unit that comes to mind when you think of "timeseries" data. Ex. The CPU level for a single host in a datacenter over a period of time could be represented as a series with id "<HOST_IDENTIFIER>.system.cpu.utilization" and a vector of tuples in the form of (<TIMESTAMP>, <CPU_LEVEL>)
5. "Blocks" belong to a series and are central to M3DB's design. In order to understand blocks, we must first understand that one of M3DB's biggest strengths as a timeseries database (as opposed to using a more general-purpose horizontally scalable, distributed database like Cassandra) is its ability to compress time-series data resulting in huge memory and disk savings. This high compression ratio is implemented via a variant of MTSZ (Link to Guerilla paper and M3TSZ and explain slight difference) which is an algorithm for implementing streaming timeseries compression. A "block" then is simply a sealed (no longer writable) stream of compressed timeseries data. The compression ratio will vary depending on the workload and configuration, but with careful tuning its possible to encode data with an average of 1.4 bytes per datapoint. The compression comes with a few caveats though, namely that a compressed block cannot be scanned or indexed into, the entire block must be decompressed from the beginning until you reach the datapoint you're looking for.

(TODO: Diagram this out)

If M3DB kept everything in memory (and in fact, early versions of it did), than you could conceptually think of it as having a single top-level database which contained an internal map of <NAMEPSACE_ID>:<NAMESPACE_OBJECT> and each
namespace would have an internal map of <SHARD_NUMBER>:<SHARD_OBJECT> and each shard would have an internal map of
<SERIES_ID (in reality we use a hash)>:<SERIES_OBJECT> and each series would have an internal map of <BLOCK_START_TIME:BLOCK_OBJECT> where a block object is a thin wrapper around a compressed block of M3TSZ encoded data.

In fact, even though M3DB does implement persistent storage, the in-memory datastructures described above conceptually map closely to the actual implementation of the in-memory structures.

### Persistent storage

While in-memory databases can be useful (and M3DB supports operating in a memory-only mode), with large volumes of data it becomes prohibitively expensive to keep all of the data in memory. In addition, monitoring timeseries data often follows a "write-once, read-never" pattern where less than a few percent of all the data thats ever stored is ever read. With that type of workload, its wasteful to keep all of that data in memory when it could be persisted on disk and retrieved when required (with an appropriate caching policy for frequently accessed data).

Like many databases, M3DB takes a two-pronged approach to persistent storage:

1) All writes are persisted to a commitlog (the commitlog can be configured to fsync every write, or optionally batch writes together which is much faster but leaves open the possibility of small amounts of data loss in the case of a catastrophic failure). The commitlog is completely uncompressed and exists only to recover "unflushed" data in the case of a database shutdown (intentional or not.)
2) Periodically (based on the configured blocksize) all "active" blocks are "sealed" (marked as immutable) and written out to disk into "fileset" files. These files are highly compressed and can be indexed into via their complementary index files.

#### Fileset files

A fileset has the following files:

* **Info file:** Stores the block time window start and size and other important metadata about the fileset volume.
* **Summaries file:** Stores a subset of the index file for purposes of keeping the contents in memory and jumping to section of the index file that within a few pages of linear scanning can find the series that is being looked up.
* **Index file:** Stores the series metadata and location of compressed stream in the data file for retrieval.
* **Data file:** Stores the series compressed data streams.
* **Bloom filter file:** Stores a bloom filter bitset of all series contained in this fileset for quick knowledge of whether to attempt retrieving a series for this fileset volume.
* **Digests file:** Stores the digest checksums of the info file, summaries file, index file, data file and bloom filter file in the fileset volume for integrity verification.
* **Checkpoint file:** Stores a digest of the digests file and written at the succesful completion of a fileset volume being persisted, allows for quickly checking if a volume was completed.

```
                                                     ┌─────────────────────┐   
┌─────────────────────┐  ┌─────────────────────┐     │     Index File      │   
│      Info File      │  │   Summaries File    │     │   (sorted by ID)    │   
├─────────────────────┤  │   (sorted by ID)    │     ├─────────────────────┤   
│- Block Start        │  ├─────────────────────┤  ┌─>│- Idx                │   
│- Block Size         │  │- Idx                │  │  │- ID                 │   
│- Entries (Num)      │  │- ID                 │  │  │- Size               │   
│- Major Version      │  │- Index Entry Offset ├──┘  │- Checksum           │   
│- Summaries (Num)    │  └─────────────────────┘     │- Data Entry Offset  ├──┐
│- BloomFilter (K/M)  │                              └─────────────────────┘  │
└─────────────────────┘                                                       │
                         ┌─────────────────────┐  ┌───────────────────────────┘
┌─────────────────────┐  │  Bloom Filter File  │  │                            
│    Digests File     │  ├─────────────────────┤  │  ┌─────────────────────┐   
├─────────────────────┤  │- Bitset             │  │  │      Data File      │   
│- Info file digest   │  └─────────────────────┘  │  ├─────────────────────┤   
│- Summaries digest   │                           │  │List of:             │   
│- Index digest       │                           └─>│  - Marker (16 bytes)│   
│- Data digest        │                              │  - ID               │   
│- Bloom filter digest│                              │  - Data (size bytes)│   
└─────────────────────┘                              └─────────────────────┘   
                                                                               
┌─────────────────────┐                                                        
│   Checkpoint File   │                                                        
├─────────────────────┤                                                        
│- Digests digest     │                                                        
└─────────────────────┘                                                        
                                                                               
```

In the diagram above you can see that the data file stores compressed blocks for a given shard / block start combination. The index file (which is sorted by ID and thus can be binary searched or scanned) can be used to find the offset of a specific ID.

Now that we understand the in-memory datastructures, as well as the files on disk, we can discuss how they interact to create a database where only a portion of the data exists in memory at any given time.

The first thing to discuss is when does data move from memory to the filesystem? The commitlog is continuously being written to, but eventually we need to write the data out into Fileset files in order to facilitate efficient storage and retrieval. This is where the configurable "blocksize" comes into play.

The blocksize is simply a duration of time that dictates how long active writes will be compressed (in a streaming manner) in memory before being "sealed" (marked as immutable) and flushed to disk. Lets use a blocksize of two hours as an example.

If the blocksize is set to two hours, then all writes for all series for a given shard will be buffered in memory for two hours at a time. Datapoints will be compressed using M3TSZ as they arrive (an active M3TSZ "encoder" object will be held in memory for each series), and at the end of the two hour period all of the fileset files discussed above will be generates, written to disk, and then the in-memory datastructures can be released and replaces with new ones for the new block.

If the database is stopped for any reason inbetween "flushes" (writing fileset files out to disk), then when the node is started back up those writes will need to be recovered by reading the commitlog or streaming in the data from a peer responsible for the same shard (if the replication factor is larger than 1.)

Generally speaking, this means that your commitlog retention needs to be at least as larger as your blocksize to prevent dataloss during node failure.

The blocksize parameter is the most important variable that needs to be tuned for your workload. A small blocksize will mean more frequent flushing and a smaller memory footprint for the data that is being actively compressed, but it will also reduce the compression ratio and your data will take up more space on disk.

## Lifecycle of a write

We now have enough context of M3DB's architecture to discuss the lifecycle of a write. A write begins when an M3DB client calls the Write or WriteBatch (TODO: API name?) endpoint on M3DB's thrift server. The write itself will contain the following information:

1. The namespace
2. The series ID (byte blob)
3. The timestamp
4. The value itself

M3DB will consult the database object to check if the namespace exists, and if it does, then it will hash the series ID to determine which shard it belongs to. If the node receiving the write owns that shard, then it will lookup the series in the shard object. If the series exists, then it will lookup the series corresponding encoder (TODO: Talk about out of order writes) and encode the datapoint into the compressed stream. If the encoder doesn't exist (no writes for this series have occurred yet as part of this block) then a new encoder will be allocated and it will begin a compressed M3TSZ stream with that datapoint.

At the same time, the write will be appended to the commitlog queue (and depending on the commitlog configuration immediately fsync'd to disk or batched together with other writes and flushed out all at once).

The write will exist only in this "active buffer" and the commitlog until the block ends and is flushed to disk, at which point the write will exist in a fileset file for efficient storage and retrieval later and the commitlog entry can be garbage collected.

## Lifecycle of a read

A read begin when an M3DB client calls the Read or ReadBatch (TODO: API name?) endpoint on M3DB's thrift server. The read request will contain the following information:

1. The namespace
2. The series ID (byte blob)
3. The period of time being requested (start and end)

M3DB will consult the database object to check if the namespace exists, and if it does, then it will hash the series ID to determine which shard it belongs to. If the node receiving the read owns that shard, then M3DB needs to determine two things:

1. Does the series exist? and if it does
2. Does the data exist in an "active buffer" (actively being compressed by an encoder), cached in-memory, on disk, or some combination of all three?

Determining whether the series exists is simple. M3DB looks up the series in the shard object. If it exists, then the series exists. If it doesn't, then M3DB consults an in-memory bloom filter(s) for that shard / block start combination(s) to determine if the series exists on disk.

If the series exists, then for every block that the request spans, M3DB needs to consolidate data from the active buffers, in-memory cache, and fileset files (disk).

Lets imagine a read for a given series that requests the last 8 hours worth of data, and an M3DB namespace that is configured with a blocksize of 2 hours (I.E we need to find 4 different blocks.)

If the current time is 8PM, then the location of the requested blocks might be as follows:

(TODO: Make this a nice diagram)

[12PM - 2PM (Fileset file)] - Because this block was sealed and flushed and hasn't been read recently

[2PM - 4PM (Fileset file)] - Because this block was sealed and flushed and hasn't been read recently

[4PM - 6PM (In-memory cache)] - Because this block was read from disk recently and cached

[6PM - 8PM (active buffer)] - Because this block hasn't been sealed and flushed to disk yet


Then M3DB will need to consolidate:

1) The not-yet-sealed block from the active buffer / encoders
2) 
M3DB will hash the series ID to determine which shard it belongs to,