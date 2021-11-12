---
title: "Flushing"
weight: 2
---

## Overview

Flushing is the process by which m3aggregator instances output the aggregated time series data 
(by using [m3msg](https://github.com/m3db/m3/tree/master/src/msg#readme) protocol).
There are two targets to which the data is flushed:
1. Persistence: the aggregated data is flushed to `m3coordinator` which then 
   persists it in `m3db`.
1. Forwarding: intermediate aggregation data is flushed to other nodes of `m3aggregator`
   (in a multi-node setup) for further processing. This is necessary for rollup rule 
   processing - eg. when a label is dropped, the resulting time series has a new id
   which potentially maps to a different shard (which may be owned by a different node).

## Flushing coordination between leaders and followers 

In a replicated setup, only the [leader nodes are flushing](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/aggregator/leader_flush_mgr.go#L475) 
the data (both persistence and forwarding). 
After each flush, for every combination of shard/metrics type/aggregation window the leader node 
[updates last flushed timestamp](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/aggregator/leader_flush_mgr.go#L183-L188) 
in etcd. 

Follower nodes are [watching the changes of last flushed 
timestamps](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/aggregator/follower_flush_mgr.go#L563-L567) 
for the shardset that they own. The data that has been flushed by the leader node 
is considered as persisted, and the followers can [discard](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/aggregator/follower_flush_mgr.go#L590)
the corresponding data they have accumulated for the given shard/metrics type/aggregation window.

If, for some reason, a follower node does not receive last flush timestamp updates from
the leader, it will keep the aggregated data for a while, but the follower will go into
[forced flush mode](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/aggregator/follower_flush_mgr.go#L195-L197) 
after the time period defined by `maxBufferSize` and start discarding the 
accumulated data in order to reclaim the memory. The default value for this setting is 5 minutes:
```yaml
aggregator:
  flushManager:
    maxBufferSize: 5m
```

When a follower gets promoted to a leader, it initially flushes time windows it has buffered,
following the last flush timestamp reported by the previous leader via etcd. 

## Shard cutover/cutoff

[Cutoff/cutover](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/cluster/generated/proto/placementpb/placement.proto#L71-L72) 
are timestamps that can be used to control flushing from the given shard.
They are fields that are defined for each shard in the placement structure.

If the shard has its cutover field set to some timestamp value, the shard will only
[start flushing](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/aggregator/list.go#L313) 
once the wall clock will go past the given cutover timestamp. Until this happens, any aggregated
data gets discarded.

Similarly, if the shard has its cutoff field set to some value, the shard will 
[stop flushing](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/aggregator/list.go#L323-L330) 
once the wall clock will go past the given cutoff timestamp.

If the shard does not have cutover/cutoff fields it will flush indefinitely.
