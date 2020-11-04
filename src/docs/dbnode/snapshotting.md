# Snapshotting

Index and data snapshotting documentation

## Consistency model

Both data and index snapshotting happens in the warm flush life cycle. Each snapshot run is assigned a UUID for snapshot ID which is written to a snapshot metdata file. A snapshot metadata file is written each snapshot run regardless of whether or not any index or data filesets were written to disk. For each block start within retention, we snapshot any in-memory data (no work is done if there is no data in-mem).

We perform data and index snapshot cleanup before we perform snapshotting in the warm flush lifecycle. The cleanup and snapshot processes cannot be run concurrently as the cleanup process relies on the latest snapshot UUID to perform cleanup. The snapshot cleanup logic for index and data snapshots are as follows:

- Data snapshots: Delete everything but the latest snapshot UUID.
- Index snapshots: Delete everything up to the snapshot version loaded into memory (happens when we bootstrap from index snapshots) if set or delete everything but the latest snapshot UUID.

## Eviction of loaded index snapshots

There are both warm and cold snapshots which are differentiated by their index volume type.
```
// SnapshotColdIndexVolumeType holds cold index snapshot data.
SnapshotColdIndexVolumeType IndexVolumeType = "snapshot_cold"
// SnapshotWarmIndexVolumeType holds warm index snapshot data.
SnapshotWarmIndexVolumeType IndexVolumeType = "snapshot_warm"
```

Warm snapshots are evicted from memory on a per block basis at the end of a warm flush for an index block and cold snapshots are evicted at the end of a cold flush for an index block.

## Stale index snapshots

We can have stale index snapshots in between when a successful cold and/or warm flush occurs and when the next index snapshot gets written to disk.

For cold index blocks, we will flush the stale snapshot from memory during the next index cold flush. For warm index blocks, we check to see if the index block has been sealed and if a successful warm flush has occured when we add bootstrap results in the index. If we've already successfully warm flushed a block that means any warm index snapshots are considered "stale" and are not loaded as they will never get flushed if they do.
