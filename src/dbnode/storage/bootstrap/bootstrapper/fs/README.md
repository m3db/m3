# fs bootstrapper

The fs bootstrapper inspects both data and index info files on disk and marks requested shard time ranges
for each as fulfilled respectively when it finds persisted data for either.

The fs bootstrapper *only* bootstraps from persisted data on disk. It passes along unfulfilled
shard time ranges to the next bootstrapper (as does every bootstrapper).

## TSDB data on disk missing index data

There are a few cases where TSDB blocks may be missing an index block on disk.
  1. TSDB blocks smaller than index blocks
    - TSDB blocks can exist on disk that don't cover the entire index block
  2. Node crash between a succesful warm TSDB flush(es) (for the entire index block)
     and successful index flush.
  3. Crash after streaming TSDB blocks from peer during peers bootstrapping.
    - Shard will still be marked as initializing.

We handle case 1 by relying on bootstrapping from index snapshots/commitlogs to load in-mem index data.
We handle case 2 by just waiting for a successful index warm flush to complete.
We handle case 3 by passing unfulfilled index shard time ranges along to the peers bootstrapper (for uninitialized shards) so that it can build index segments for TSDB blocks missing index data.

Also, w.r.t. cold flush, we don't write out checkpoint files for cold flushed TSDB data until
index data has been successfully persisted to disk so cold flushed data will never be missing index data.

Additional notes on when data becomes visible on disk:
  - TSDB data becomes visible on disk after each shard.WarmFlush() op completes successfully
  - Index data becomes visible on disk after each index.flushBlock() op completes successfully
