# Storage

## Overview

Time series values are stored in compressed streams in filesets, one per shard and block time window size.  They are flushed to disk after a block time window becomes unreachable, that is the end of the time window for that block can no longer be written to.  If a process is killed before it has a chance to flush the data for the current time window to disk it must be restored from the commit log (or a peer that is responsible for the same shard if replication factor is larger than 1.)

## Filesets

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
