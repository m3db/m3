## M3DB Commit Logs

### Integrity Levels

There are two integrity levels available for M3DB commit logs:

- **Synchronous:** all write operations must wait until they have finished writing an entry in the commit log to complete.
- **Behind:** all write operations must finish enqueueing an entry to the commit log write queue to complete.

Depending on the data loss requirements consumers can choose either integrity level.

### Properties

Commit logs will be stamped by the start time and aligned and rotated by the block size.  To restore data for an entire block requires the commit logs from the previous block size period, the current block size period, and the future block size. This is to account for the buffer past data and the buffer future data.

### Structure

Commit logs for a given block are kept in a single file. An info structure keeping metadata is written at the header of the file and all consequent entries have a log structure, optionally containing metadata describing the series if it's the first time a commit log with the given series index appears.

The structures have the following properties:

```
CommitLogInfo struct {
  start int64
  duration int64
  index int64
}

CommitLog struct {
  created int64
  idx uint64
  metadata bytes
  timestamp int64
  value double
  unit uint32
  annotation bytes
}

CommitLogMetadata struct {
  id string
  shard uint32
}
```

### Garbage Collection

Commit logs are garbage collected. This happens when all relevant blocks have already been flushed to disk, i.e. all blocks within the retention period for which the commit log could contain data.