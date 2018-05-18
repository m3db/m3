# Commit Logs

## Overview

M3DB has a commit log that is equivalent to the commit log or write-ahead-log in other databases. The commit logs are completely uncompressed (no M3TSZ encoding), and there is one per database (multiple namespaces in a single process will share a commit log.)

## Integrity Levels

There are two integrity levels available for commit logs:

- **Synchronous:** write operations must wait until it has finished writing an entry in the commit log to complete.
- **Behind:** write operations must finish enqueueing an entry to the commit log write queue to complete.

Depending on the data loss requirements users can choose either integrity level.

### Properties

Commit logs will be stamped by the start time, aligned and rotated by a configured time window size. To restore data for an entire block you will require the commit logs from all time commit logs that overlap the block size with buffer past subtracted from the bootstrap start range and buffer future extended onto the bootstrap end range.

### Structure

Commit logs for a given time window are kept in a single file. An info structure keeping metadata is written to the header of the file and all consequent entries are a repeated log structure, optionally containing metadata describing the series if it's the first time a log entry for a given series appears.

The structures can be conceptually described as:

```
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

### Garbage Collected

Commit logs are garbage collected after all blocks within the retention period in which data inside the commit logs could be applicable have already been flushed to disk as immutable compressed filesets.

### Compaction

There is currently no compaction process for commitlogs. They are deleted once they fall out of their configurable retention period *or* all the [fileset files](storage.md) for that period are flushed.