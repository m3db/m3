# storage

Storage related documentation.

## Flush consistency model

Warm and cold flush each have their own independent lifecycle:

Warm flush:
  - warm flush cleanup
    - expired/duplicate index files
    - inactive snapshot/namespace files.
    - index/data snapshots
  - data warm flush
  - rotate commit log
  - data and index snapshot
    - drops rotated commit log when we are done
  - index warm flush

Cold flush:
  - cold flush cleanup
    - out of retention/compacted data files
    - data for no longer owned shards
  - data cold flush
    - rotate cold mutable index segments
    - flush cold tsdb data and write most files to disk (except checkpoint files)
    - flush cold index data to disk and reload
    - evict rotated cold mutable index segments
    - write tsdb checkpoint files (completes the tsdb cold flush lifecycle)

Since we rotate the commit log before we perform a data cold flush and only drop the rotate commit log after data snapshotting is done we guarantee that no writes will be lost if the node crashes. After data cold flush completes, any new cold writes will exist in the active commit log (and not be dropped) when data snapshotting finishes. This is why data snapshotting only needs to snapshot warm data blocks (that need to be flushed).

## Snapshotting consistency model

Both data and index snapshotting happens in the warm flush life cycle. Each snapshot run is assigned a UUID for snapshot ID which is written to a snapshot metdata file. A snapshot metadata file is written each snapshot run regardless of whether or not any index or data filesets were written to disk. For each block start within retention, we snapshot any in-memory data (no work is done if there is no data in-mem).

We perform data and index snapshot cleanup before we perform snapshotting in the warm flush lifecycle. The cleanup and snapshot processes cannot be run concurrently as the cleanup process relies on the latest snapshot UUID to perform cleanup. The snapshot cleanup logic for index and data snapshots are as follows:

data snapshots - delete everything but the latest snapshot UUID
index snapshots - delete everything up to the snapshot version loaded into memory (happens when we bootstrap from index snapshots) if set or delete everything but the latest snapshot UUID

## Future plans

We will be migrating to 1:1 sizing of index and TSDB blocks.
