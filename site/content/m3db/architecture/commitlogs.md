---
title: "Commit Logs And Snapshot Files"
menuTitle: "Commit Logs"
weight: 5
---

## Overview

M3DB has a commit log that is equivalent to the commit log or write-ahead-log in other databases. The commit logs are completely uncompressed (no M3TSZ encoding), and there is one per database (multiple namespaces in a single process will share a commit log.)

## Integrity Levels

There are two integrity levels available for commit logs:

-   **Synchronous:** write operations must wait until it has finished writing an entry in the commit log to complete.
-   **Behind:** write operations must finish enqueueing an entry to the commit log write queue to complete.

Depending on the data loss requirements users can choose either integrity level.

### Properties

Commit logs will be stamped by the start time, aligned and rotated by a configured time window size. To restore data for an entire block you will require the commit logs from all time commit logs that overlap the block size with buffer past subtracted from the bootstrap start range and buffer future extended onto the bootstrap end range.

### Structure

Commit logs for a given time window are kept in a single file. An info structure keeping metadata is written to the header of the file and all consequent entries are a repeated log structure, optionally containing metadata describing the series if it's the first time a log entry for a given series appears.

The structures can be conceptually described as:

```golang
CommitLogInfo {
  start int64
  duration int64
  index int64
}

CommitLog {
  created int64
  index uint64
  metadata bytes
  timestamp int64
  value float64
  unit uint32
  annotation bytes
}

CommitLogMetadata {
  id bytes
  namespace bytes
  shard uint32
}
```

### Compaction / Snapshotting

Commit log files are compacted via the snapshotting proccess which (if enabled at the namespace level) will snapshot all data in memory into compressed files which have the same structure as the [fileset files](/v0.15.17/docs/m3db/architecture/storage) but are stored in a different location. Once these snapshot files are created, then all the commit log files whose data are captured by the snapshot files can be deleted. This can result in significant disk savings for M3DB nodes running with large block sizes and high write volume where the size of the (uncompressed) commit logs can quickly get out of hand.

In addition, since the snapshot files are already compressed, bootstrapping from them is much faster than bootstrapping from raw commit log files because the individual datapoints don't need to be decoded and then M3TSZ encoded. The M3DB node just needs to read the raw bytes off disk and load them into memory.

### Cleanup

Commit log files are automatically deleted once all the data they contain has been flushed to disk as immutable compressed filesets _or_ all the data they contain has been captured by a compressed snapshot file. Similarly, snapshot files are deleted once all the data they contain has been flushed to disk as filesets.
