# bootstrapper

The collection of bootstrappers comprise the task executed when bootstrapping a node.

## A high level overview of bootstrapping process

During the bootstrapping process, the list of configured bootstrappers is iterated, invoking the bootstrap routine of each of them. Each bootstrapper is given time ranges for which data needs to be bootstrapped. It acknowledges, retrieves, processes and/or persists the data that is available to it, and returns a subset of requested time ranges that it was unable to fulfill. 

If there are any errors or unfulfilled time ranges after going through all the configured bootstrappers, the process is retried. 

See pseudo-code below for more details.

```python
bootstrappers = {filesystem, commitlog, peer, uninitialized}

while true:
    namespaces = getOwnedNamespaces()
    shards = filterNonBootstrapped(getOwnedShards()) # NOTE: peer bootstrapper takes INITIALIZING shards

    # shard time ranges which needs to be bootstrapped
    persistRange, memoryRange =
        timeRangesFromRetentionPeriodToCurrentBlock(namespaces, shards)

    bootstrapResult = newBootstrapResult()
    for r in {persistRange, memoryRange}:
        remainingRanges = r
        # iterate through the configured bootstrappers 
        for b in bootstrappers:
            availableRanges = getAvailableRanges(b, namespaces, remainingRanges)
            bootstrappedRanges = bootstrap(b, namespaces, availableRanges)
            remainingRanges -= bootstrappedRanges

        # record unfulfilled ranges
        unfulfilledRanges = remainingRanges
        updateResult(bootstrapResult, unfulfilledRanges)

    bootstrapNamespaces(namespaces, bootstrapResults)
    bootstrapShards(shards, bootstrapResult)

    if hasNoErrors(bootstrapResult) and allRangesFulfilled(bootstrapResult):
        break
```

## Bootstrappers

- `fs`: The filesystem bootstrapper, used to bootstrap as much data as possible from the local filesystem.
- `peers`: The peers bootstrapper, used to bootstrap any remaining data from peers. This is used for a full node join too.
- `commitlog`: The commit log bootstrapper, currently only used in the case that peers bootstrapping fails. Once the current block is being snapshotted frequently to disk it might be faster and make more sense to not actively use the peers bootstrapper and just use a combination of the filesystem bootstrapper and the minimal time range required from the commit log bootstrapper.
    - *NOTE*: the commitlog bootstrapper is special cased in that it runs for the *entire* bootstrappable range per shard whereas other bootstrappers fill in the unfulfilled gaps as bootstrapping progresses.
- `uninitialized`: The uninitialized bootstrapper, used to bootstrap a node when the whole cluster is new and there are no peers to fetch the data from.

The bootstrappers satisfy the `bootstrap.Source` interface. `AvailableData()` and `AvailableIndex()` methods take shard time ranges that need to be bootstrapped and return which ranges (possibly a subset) can be fulfilled by this bootstrapper. `Read()` method does the bootstrapping of the available data and index ranges.

### Filesystem

- `AvailableData()`, `AvailableIndex()` - for each shard, reads info files (or cached data of info files) and converts block start times to available ranges
- `Read()` - reads data ranges from info files without reading the data, then builds index segments and either flushes them to disk or keeps in memory

### Commit log

- `AvailableData()`, `AvailableIndex()` - checks which shards have ever reached availability (i.e. is in `Available` or `Leaving` state) and returns the whole requested time range for those shards
- `Read()` - for each shard reads the most recent snapshot file, and then reads commit log and checks-out all the series belonging to the namespaces that are being bootstrapped

### Peer

- `AvailableData()`, `AvailableIndex()` - inspects the cluster topology and returns the whole requested time range for shards which have enough available replicas to satisfy consistency requirements
- `Read()` - fetches shard data from peers and either persists it or checks-out into memory. When fetching, block checksums are compared between peers: if they match, data is retrieved from one of the peers, otherwise data from multiple peers is merged. Then it builds index segments and either flushes them to disk or keeps in memory

### Uninitialized

- `AvailableData()`, `AvailableIndex()` - for each shard, inspects its status across peers. If the number of `Initializing` replicas is higher than `Leaving`, the shard is deemed _new_ and available to be bootstrapped by this bootstrapper
- `Read()` - for each shard that is new (as described above), respond that it was fulfilled

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

### Node add

When a node is added to the cluster it is assigned shards that relieves load fairly from the existing nodes.  The shards assigned to the new node will become _INITIALIZING_, the nodes then discover they need to be bootstrapped and will begin bootstrapping the data using all replicas available.  The shards that will be removed from the existing nodes are marked as _LEAVING_.

### Node down

A node needs to be explicitly taken out of the cluster.  If a node goes down and is unavailable the clients performing reads will be served an error from the replica for the shard range that the node owns.  During this time it will rely on reads from other replicas to continue uninterrupted operation.

### Node remove

When a node is removed the shards it owns are assigned to existing nodes in the cluster.  Remaining servers discover they are now in possession of shards that are _INITIALIZING_ and need to be bootstrapped and will begin bootstrapping the data using all replicas available.
