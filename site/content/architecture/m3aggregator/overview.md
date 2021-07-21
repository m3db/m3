---
title: "Overview"
weight: 1
---

`m3aggregator` is a distributed system written in Go. It does streaming aggregation of ingested time 
series data before persisting it in m3db. The goal is to reduce the volume of
time series data stored (especially for longer retentions), which is achieved by reducing its 
cardinality and/or datapoint resolution.

`m3aggregator` is sharded for horizontal scalability and replicated (leader/
follower modes) for high availability.

The data processed is mapped to a predefined number of shards, depending on
[hash of time series id](https://github.com/m3db/m3/blob/0865ebc80e85234b00532f93521438856883da9c/src/aggregator/sharding/hash.go#L89). 
Every node of `m3aggregator` is responsible for a single
shardset which contains one or more shards assigned to it. The assignment of
shards to the shardsets, and shardsets to individual nodes is part of the 
`m3aggregator` placement which is stored in etcd as a 
[binary protobuf struct](https://github.com/m3db/m3/blob/master/src/cluster/generated/proto/placementpb/placement.proto).

The communication protocol used by `m3aggregator` is [m3msg](https://github.com/m3db/m3/tree/master/src/msg#readme).
`m3aggregator` receives the input data from [m3coordinator](/docs/architecture/m3coordinator).
See [Flushing](/docs/architecture/m3aggregator/flushing) for how the aggregator outputs the 
aggregated data (also for the inter-node communication).
