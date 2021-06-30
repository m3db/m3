---
title: "Leader Election"
weight: 2
---

A single `m3aggregator` node for every shardset is elected to be a leader. Both leader and
follower nodes are receiving the writes and performing the aggregation. 
The main difference between the leader and the follower is that the leader node
is responsible for flushing (persisting) the data it has aggregated 
(see [Flushing](/docs/architecture/m3aggregator/flushing.md) for more details).

