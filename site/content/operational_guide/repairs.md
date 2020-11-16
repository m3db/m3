---
title: "Background Repairs"
weight: 14
---

**Note:** This feature is in beta and only available for use with M3DB when run with the inverted index off. It can be run with the inverted index on however metrics will not be re-indexed if they are repaired so will be invisible to that node for queries.

## Overview

Background repairs enable M3DB to eventually reach a consistent state such that all nodes have identical view.
An M3DB cluster can be configured to repair itself in the background. If background repairs are enabled, M3DB nodes will continuously scan the metadata of other nodes. If a mismatch is detected, affected nodes will perform a repair such that each node in the cluster eventually settles on a consistent view of the data.

A repair is performed individually by each node when it detects a mismatch between its metadata and the metadata of its peers. Each node will stream the data for the relevant series, merge the data from its peers with its own, and then write out the resulting merged dataset to disk to make the repair durable. In other words, there is no coordination between individual nodes during the repair process, each node is detecting mismatches on its own and performing a "best effort" repair by merging all available data from all peers into a new stream.

## Configuration

The feature can be enabled by adding the following configuration to `m3dbnode.yml` under the `db` section:

```yaml
db:
  ... (other configuration)
  repair:
    enabled: true
```

By default M3DB will limit the amount of repaired data that can be held in memory at once to 2GiB. This is intended to prevent the M3DB nodes from streaming data from their peers too quickly and running out of memory. Once the 2GiB limit is hit the repair process will throttle itself until some of the streamed data has been flushed to disk (and as a result can be evicted from memory). This limit can be overriden with the following configuration:

```yaml
db:
  ... (other configuration)
  limits:
    maxOutstandingRepairedBytes: 2147483648 # 2GiB
```

In addition, the following two optional fields can also be configured:

```yaml
db:
  ... (other configuration)
  repair:
    enabled: true
    throttle: 10s
    checkInterval: 10s
```

The `throttle` field controls how long the M3DB node will pause between repairing each shard/blockStart combination and the `checkInterval` field controls how often M3DB will run the scheduling/prioritization algorithm that determines which blocks to repair next. In most situations, operators should omit these fields and rely on the default values.

## Caveats and Limitations

1.  Background repairs do not currently support M3DB's inverted index; as a result, it can only be used for clusters / namespaces where the indexing feature is disabled.
2.  Background repairs will wait until (`block start` + `block size` + `buffer past`) has elapsed before attempting to repair a block. For example, if M3DB is configured with a 2 hour block size and a 20 minute buffer past that M3DB will not attempt to repair the `12PM->2PM` block until at least `2:20PM`. This limitation is in place primarily to reduce "churn" caused by repairing mutable data that is actively being modified. **Note**: This limitation has no impact or negative interaction with M3DB's cold writes feature. In other words, even though it may take some time before a block becomes available for repairs, M3DB will repair the same block repeatedly until it falls out of retention so mismatches between nodes that were caused by "cold" writes will still eventually be repaired.
