# Flush

Index and data flush documentation.

## Consistency model

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
