---
title: "Storage"
weight: 4
---

## Overview

The primary unit of long-term storage for M3DB are fileset files which store compressed streams of time series values, one per shard block time window size.

They are flushed to disk after a block time window becomes unreachable, that is the end of the time window for which that block can no longer be written to.  If a process is killed before it has a chance to flush the data for the current time window to disk it must be restored from the commit log (or a peer that is responsible for the same shard if replication factor is larger than 1.)

## FileSets

A fileset has the following files:

* **Info file:** Stores the block time window start and size and other important metadata about the fileset volume.
* **Summaries file:** Stores a subset of the index file for purposes of keeping the contents in memory and jumping to section of the index file that within a few pages of linear scanning can find the series that is being looked up.
* **Index file:** Stores the series metadata, including tags if indexing is enabled, and location of compressed stream in the data file for retrieval.
* **Data file:** Stores the series compressed data streams.
* **Bloom filter file:** Stores a bloom filter bitset of all series contained in this fileset for quick knowledge of whether to attempt retrieving a series for this fileset volume.
* **Digests file:** Stores the digest checksums of the info file, summaries file, index file, data file and bloom filter file in the fileset volume for integrity verification.
* **Checkpoint file:** Stores a digest of the digests file and written at the succesful completion of a fileset volume being persisted, allows for quickly checking if a volume was completed.

```
                                                     ┌───────────────────────┐
┌─────────────────────┐  ┌─────────────────────┐     │     Index File        │
│      Info File      │  │   Summaries File    │     │   (sorted by ID)      │
├─────────────────────┤  │   (sorted by ID)    │     ├───────────────────────┤
│- Block Start        │  ├─────────────────────┤  ┌─>│- Idx                  │
│- Block Size         │  │- Idx                │  │  │- ID                   │
│- Entries (Num)      │  │- ID                 │  │  │- Size                 │
│- Major Version      │  │- Index Entry Offset ├──┘  │- Checksum             │
│- Summaries (Num)    │  └─────────────────────┘     │- Data Entry Offset    ├──┐
│- BloomFilter (K/M)  │                              │- Encoded Tags         │  │
│- Snapshot Time      │                              │- Index Entry Checksum │  │
│- Type (Flush/Snap)  │                              └───────────────────────┘  │
│- Snapshot ID        │                                                         │
│- Volume Index       │                                                         │
│- Minor Version      │                                                         │
└─────────────────────┘                                                         │
                                                                                │
                         ┌─────────────────────┐  ┌─────────────────────────────┘
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

FileSet files will be kept for every shard / block start combination that is within the retention period. Once the files fall out of the period defined in the configurable namespace retention period they will be deleted.
