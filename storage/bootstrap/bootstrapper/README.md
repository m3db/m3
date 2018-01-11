# bootstrapper

The collection of bootstrappers comprise the task executed when bootstrapping a node.

## Bootstrappers

- `fs`: The filesystem bootstrapper, used to bootstrap as much data as possible from the local filesystem.
- `peers`: The peers bootstrapper, used to bootstrap any remaining data from peers. This is used for a full node join too.
- `commitlog`: The commit log bootstrapper, currently only used in the case that peers bootstrapping fails. Once the current block is being snapshotted frequently to disk it might be faster and make more sense to not actively use the peers bootstrapper and just use a combination of the filesystem bootstrapper and the minimal time range required from the commit log bootstrapper.

## Cache policies

The tasks carried out by each bootstrapper vary a lot on the series cache policy being used.

### CacheAll series cache policy

For the cache all policy the filesystem bootstrapper will load all series and all the data for each block and return the entire set of data. This will keep every series and series block on heap.

The peers bootstrapper similarly bootstraps all the data from peers that the filesystem does not have and returns the entire set of data fetched.

### RecentlyRead series cache policy

For the recently read policy the filesystem bootstrapper will simply fulfill the time ranges requested matching without actually loading the series and blocks from the files it discovers.  This relies on data been fetched lazily from the filesystem when data is required for a series that does not live on heap.

The peers bootstrapper will bootstrap all time ranges requested, and if performing an incremental bootstrap for a time range will write the data to disk and then remove the results from memory. An incremental bootstrap is used for any data that is immutable at the time that bootstrapping commences. For time ranges that are mutable the series and blocks are returned directly as a result from the bootstrapper.
