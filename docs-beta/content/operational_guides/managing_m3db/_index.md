---
title: "Managing M3DB"
date: 2020-04-21T21:00:08-04:00
draft: true
---

M3DB, a distributed time series database
About
M3DB, inspired by Gorilla and Cassandra, is a distributed time series database released as open source by Uber Technologies. It can be used for storing realtime metrics at long retention.
Here are some attributes of the project:
Distributed time series storage, single nodes use a WAL commit log and persists time windows per shard independently
Cluster management built on top of etcd
Built-in synchronous replication with configurable durability and read consistency (one, majority, all, etc)
M3TSZ float64 compression inspired by Gorilla TSZ compression, configurable as lossless or lossy
Arbitrary time precision configurable from seconds to nanoseconds precision, able to switch precision with any write
Configurable out of order writes, currently limited to the size of the configured time window's block size
Current Limitations
Due to the nature of the requirements for the project, which are primarily to reduce the cost of ingesting and storing billions of timeseries and providing fast scalable reads, there are a few limitations currently that make M3DB not suitable for use as a general purpose time series database.
The project has aimed to avoid compactions when at all possible, currently the only compactions M3DB performs are in-memory for the mutable compressed time series window (default configured at 2 hours). As such out of order writes are limited to the size of a single compressed time series window. Consequently backfilling large amounts of data is not currently possible.
The project has also optimized the storage and retrieval of float64 values, as such there is no way to use it as a general time series database of arbitrary data structures just yet.

Architecture
Overview
M3DB is written entirely in Go and does not have any required dependencies. For larger deployments, one may use an etcd cluster to manage M3DB cluster membership and topology definition.
High Level Goals
Some of the high level goals for the project are defined as:
Monitoring support: M3DB was primarily developed for collecting a high volume of monitoring time series data, distributing the storage in a horizontally scalable manner and most efficiently leveraging the hardware. As such time series that are not read frequently are not kept in memory.
Highly configurable: Provide a high level of configuration to support a wide set of use cases and runtime environments.
Variable durability: Providing variable durability guarantees for the write and read side of storing time series data enables a wider variety of applications to use M3DB. This is why replication is primarily synchronous and is provided with configurable consistency levels, to enable consistent writes and reads. It must be possible to use M3DB with strong guarantees that data was replicated to a quorum of nodes and that the data was durable if desired.
Storage Engine Overview
M3DB is a time series database that was primarily designed to be horizontally scalable and able to handle high data throughput.
Time Series Compression
One of M3DB's biggest strengths as a time series database (as opposed to using a more general-purpose horizontally scalable, distributed database like Cassandra) is its ability to compress time series data resulting in huge memory and disk savings. There are two compression algorithms used in M3DB: M3TSZ and protobuf encoding.
M3TSZ
M3TSZ is used when values are floats. A variant of the streaming time series compression algorithm described in Facebook's Gorilla paper, it achieves a high compression ratio. The compression ratio will vary depending on the workload and configuration, but we found that we were able to achieve a compression ratio of 1.45 bytes/datapoint with Uber's production workloads. This was a 40% improvement over standard TSZ, which only gave us a compression ratio of 2.42 bytes/datapoint under the same conditions.
Protobuf Encoding
For more complex value types, M3DB also supports generic Protobuf messages with a few exceptions. The algorithm takes on a hybrid approach and uses different compression schemes depending on the field types within the Protobuf message.
Details on the encoding, marshaling and unmarshaling methods can be read here.

The in-memory portion of M3DB is implemented via a hierarchy of objects:
A database of which there is only one per M3DB process. The database owns multiple namespaces.
A namespace is similar to a table in other databases. Each namespace has a unique name and a set of configuration options, such as data retention and block size (which we will discuss in more detail later). A namespace owns multiple shards.
A shard is effectively the same as a "virtual shard" in Cassandra in that it provides an arbitrary distribution of time series data via a simple hash of the series ID. A shard owns multiple series.
A series represents a sequence of time series datapoints. For example, the CPU utilization for a host could be represented as a series with the ID "host1.system.cpu.utilization" and a vector of (TIMESTAMP, CPU_LEVEL) tuples. Visualizing this example in a graph, there would a single line with time on the x-axis and CPU utilization on the y-axis. A series owns a buffer and any cached blocks.
The buffer is where all data that has yet to be written to disk gets stored in memory. This includes both new writes to M3DB and data obtained through bootstrapping. More details on the buffer is explained below. Upon flushing, the buffer creates a block of its data to be persisted to disk.
A block represents a stream of compressed time series data for a pre-configured block size, for example, a block could hold data for 6-8PM (block size of two hours). A block can arrive directly into the series only as a result of getting cached after a read request. Since blocks are in a compressed format, individual datapoints cannot be read from it. In other words, in order to read a single datapoint, the entire block up to that datapoint needs to be decompressed beforehand.
Persistent storage
While in-memory databases can be useful (and M3DB supports operating in a memory-only mode), some form of persistence is required for durability. In other words, without a persistence strategy, it would be impossible for M3DB to restart (or recover from a crash) without losing all of its data.
In addition, with large volumes of data, it becomes prohibitively expensive to keep all of the data in memory. This is especially true for monitoring workloads which often follow a "write-once, read-never" pattern where less than a few percent of all the data that's stored is ever read. With that type of workload, it's wasteful to keep all of that data in memory when it could be persisted on disk and retrieved when required.
M3DB takes a two-pronged approach to persistant storage that involves combining a commit log for disaster recovery with periodic flushing (writing fileset files to disk) for efficient retrieval:
All writes are persisted to a commit log (the commit log can be configured to fsync every write, or optionally batch writes together which is much faster but leaves open the possibility of small amounts of data loss in the case of a catastrophic failure). The commit log is completely uncompressed and exists only to recover unflushed data in the case of a database shutdown (intentional or not) and is never used to satisfy a read request.
Periodically (based on the configured block size), all data in the buffer is flushed to disk as immutable fileset files. These files are highly compressed and can be indexed into via their complementary index files. Check out the flushing section to learn more about the background flushing process.
The block size parameter is the most important variable that needs to be tuned for a particular workload. A small block size will mean more frequent flushing and a smaller memory footprint for the data that is being actively compressed, but it will also reduce the compression ratio and data will take up more space on disk.
If the database is stopped for any reason in between flushes, then when the node is started back up those writes will be recovered by reading the commit log or streaming in the data from a peer responsible for the same shard (if the replication factor is larger than one).
While the fileset files are designed to support efficient data retrieval via the series ID, there is still a heavy cost associated with any query that has to retrieve data from disk because going to disk is always much slower than accessing main memory. To compensate for that, M3DB supports various caching policies which can significantly improve the performance of reads by caching data in memory.


Storage
Overview
The primary unit of long-term storage for M3DB are fileset files which store compressed streams of time series values, one per shard block time window size.
They are flushed to disk after a block time window becomes unreachable, that is the end of the time window for which that block can no longer be written to. If a process is killed before it has a chance to flush the data for the current time window to disk it must be restored from the commit log (or a peer that is responsible for the same shard if replication factor is larger than 1.)
FileSets
A fileset has the following files:
Info file: Stores the block time window start and size and other important metadata about the fileset volume.
Summaries file: Stores a subset of the index file for purposes of keeping the contents in memory and jumping to section of the index file that within a few pages of linear scanning can find the series that is being looked up.
Index file: Stores the series metadata, including tags if indexing is enabled, and location of compressed stream in the data file for retrieval.
Data file: Stores the series compressed data streams.
Bloom filter file: Stores a bloom filter bitset of all series contained in this fileset for quick knowledge of whether to attempt retrieving a series for this fileset volume.
Digests file: Stores the digest checksums of the info file, summaries file, index file, data file and bloom filter file in the fileset volume for integrity verification.
Checkpoint file: Stores a digest of the digests file and written at the succesful completion of a fileset volume being persisted, allows for quickly checking if a volume was completed.
                                                    ┌─────────────────────┐
┌─────────────────────┐  ┌─────────────────────┐     │     Index File      │
│      Info File      │  │   Summaries File    │     │   (sorted by ID)    │
├─────────────────────┤  │   (sorted by ID)    │     ├─────────────────────┤
│- Block Start        │  ├─────────────────────┤  ┌─>│- Idx                │
│- Block Size         │  │- Idx                │  │  │- ID                 │
│- Entries (Num)      │  │- ID                 │  │  │- Size               │
│- Major Version      │  │- Index Entry Offset ├──┘  │- Checksum           │
│- Summaries (Num)    │  └─────────────────────┘     │- Data Entry Offset  ├──┐
│- BloomFilter (K/M)  │                              │- Encoded Tags       |  |
│- Snapshot Time      │                              └─────────────────────┘  │
│- Type (Flush/Snap)  │                                                       │
└─────────────────────┘                                                       │
                                                                              │
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

In the diagram above you can see that the data file stores compressed blocks for a given shard / block start combination. The index file (which is sorted by ID and thus can be binary searched or scanned) can be used to find the offset of a specific ID.
FileSet files will be kept for every shard / block start combination that is within the retention period. Once the files fall out of the period defined in the configurable namespace retention period they will be deleted.
