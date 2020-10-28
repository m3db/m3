# bootstrapper

Bootstrapping is done in the following order:
  - fs
  - commitlog
  - peers

Each bootstrapper runs through two phases (two shard time ranges). The first phase is cold data and the second phase is warm data.

## fs bootstrapper

The fs bootstrapper inspects both data and index info files on disk and marks requested shard time ranges
for each as fulfilled respectively when it finds persisted data for either.

The fs bootstrapper *only* bootstraps from persisted data (both index and TSDB) on disk. It passes along unfulfilled
shard time ranges to the next bootstrapper (as does every bootstrapper).

### TSDB data on disk missing index data

There are a few cases where TSDB blocks may be missing an index block on disk.
  1. TSDB blocks smaller than index blocks
    - TSDB blocks can exist on disk that don't cover the entire index block
  2. Node crash between a succesful warm TSDB flush(es) (for the entire index block)
     and successful index flush.
  3. Crash after streaming TSDB blocks from peer during peers bootstrapping.
    - Shard will still be marked as "initializing".

We handle case 1 by relying on bootstrapping from index snapshots/commitlogs to load in-mem index data.
We handle case 2 by just waiting for a successful index warm flush to complete.
We handle case 3 by passing unfulfilled index shard time ranges along to the peers bootstrapper (for uninitialized shards) so that it can build index segments for TSDB blocks missing index data.

Also, w.r.t. cold flush, we don't write out checkpoint files for cold flushed TSDB data until
index data has been successfully persisted to disk so cold flushed data will never be missing index data.

Additional notes on when data becomes visible on disk:
  - TSDB data becomes visible on disk after each shard.WarmFlush() op completes successfully
  - Index data becomes visible on disk after each index.flushBlock() op completes successfully

## commitlog bootstrapper

The commit log bootstrapper bootstraps both data and index snapshots across all shard time ranges (up to retention).
It bootstraps commit logs only once (commit log bootstrap results are cached after a single run).

The commitlog does not bootstrap any data for "initializing" shards and will not mark any shard time ranges for these shards as fulfilled.

## peers bootstrapper

The peers bootstrapper bootstraps shard data for "initializing" shards during topology changes (when a node has received new shards).
It also is used to bootstrap any remaining data from peers and for a full node joins.

The peers bootstrapper currently only fetches TSDB blocks from peers. It will either explicitly index these series when loading
series blocks which happens during the second bootstrapper phase (warm phase) or build and normally persist index segments for
the fetched TSDB blocks.

## Cache policies

The tasks carried out by each bootstrapper vary a lot on the series cache policy being used.

### CacheAll series cache policy

For the cache all policy the filesystem bootstrapper will load all series and all the data for each block and return the entire set of data. This will keep every series and series block on heap.

The peers bootstrapper similarly bootstraps all the data from peers that the filesystem does not have and returns the entire set of data fetched.

### RecentlyRead series cache policy

For the recently read policy the filesystem bootstrapper will simply fulfill the time ranges requested matching without actually loading the series and blocks from the files it discovers.  This relies on data been fetched lazily from the filesystem when data is required for a series that does not live on heap.

The peers bootstrapper will bootstrap all time ranges requested, and if performing a bootstrap with persistence enabled for a time range, will write the data to disk and then remove the results from memory. A bootstrap with persistence enabled is used for any data that is immutable at the time that bootstrapping commences. For time ranges that are mutable the peer bootstrapper will still write the data out to disk in a durable manner, but in the form of a snapshot, and the series and blocks will still be returned directly as a result from the bootstrapper. This enables the commit log bootstrapper to recover the data in case the node shuts down before the in-memory data can be flushed.

## Topology changes

When nodes are added to a replication group, shards are given away to the joining node. Those shards are closed and we re-bootstrap with the shards that we own.
When nodes are removed from a replication group, shards from the removed node are given to remaining nodes in a replication group. The remaining nodes in the replication group will bootstrap the "new" shards that were assigned to it.
Note that we also take writes for shards that we own while bootstrapping. However, we do not allow warm/cold flushes to happen while bootstrapping.

For example, see the following sequences:
(Node add)
- Node 1:
    - Initial bootstrap (256 shards)
    - Node add
    - Bootstrap (128 shards) // These are the remaining shards it owns.
- Node 2:
    - Node add
    - Inital bootstrap (128 shards) // These are received from Node 1

(Node remove)
- Node 1:
    - Node remove
    - Bootstrap (128 shards) // These are received form Node 2, it owns 256 now.
- Node 2:
    - Node remove
