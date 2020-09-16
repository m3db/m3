# bootstrapper

The collection of bootstrappers comprise the task executed when bootstrapping a node.

## Bootstrappers

- `fs`: The filesystem bootstrapper, used to bootstrap as much data as possible from the local filesystem.
- `peers`: The peers bootstrapper, used to bootstrap any remaining data from peers. This is used for a full node join too.
- `commitlog`: The commit log bootstrapper, currently only used in the case that peers bootstrapping fails. Once the current block is being snapshotted frequently to disk it might be faster and make more sense to not actively use the peers bootstrapper and just use a combination of the filesystem bootstrapper and the minimal time range required from the commit log bootstrapper.
    - *NOTE*: the commitlog bootstrapper is special cased in that it runs for the *entire* bootstrappable range per shard whereas other bootstrappers fill in the unfulfilled gaps as bootstrapping progresses.

## Cache policies

The tasks carried out by each bootstrapper vary a lot on the series cache policy being used.

### CacheAll series cache policy

For the cache all policy the filesystem bootstrapper will load all series and all the data for each block and return the entire set of data. This will keep every series and series block on heap.

The peers bootstrapper similarly bootstraps all the data from peers that the filesystem does not have and returns the entire set of data fetched.

### RecentlyRead series cache policy

For the recently read policy the filesystem bootstrapper will simply fulfill the time ranges requested matching without actually loading the series and blocks from the files it discovers.  This relies on data been fetched lazily from the filesystem when data is required for a series that does not live on heap.

The peers bootstrapper will bootstrap all time ranges requested, and if performing a bootstrap with persistence enabled for a time range, will write the data to disk and then remove the results from memory. A bootstrap with persistence enabled is used for any data that is immutable at the time that bootstrapping commences. For time ranges that are mutable the peer bootstrapper will still write the data out to disk in a durable manner, but in the form of a snapshot, and the series and blocks will still be returned directly as a result from the bootstrapper. This enables the commit log bootstrapper to recover the data in case the node shuts down before the in-memory data can be flushed.

## Topology Changes

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
