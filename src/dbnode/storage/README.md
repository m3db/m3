# storage

Storage related documentation.

## Flush consistency model

Flush occurs in the following steps:
  - data warm flush
  - rotate commit log
  - data cold flush
  - data snapshot
    - drops rotated commit log when we are done
  - index flush

Since we rotate the commit log before we perform a data cold flush and only drop the rotate commit log after data snapshotting is done we guarantee that no writes will be lost if the node crashes. After data cold flush completes, any new cold writes will exist in the active commit log (and not be dropped) when data snapshotting finishes. This is why data snapshotting only needs to snapshot warm data blocks (that need to be flushed).
