---
title: "Components"
weight: 1
---

## M3 Coordinator

M3 Coordinator is a service that coordinates reads and writes between upstream systems, such as Prometheus, and M3DB. It is a bridge that users can deploy to access the benefits of M3DB such as long term storage and multi-DC setup with other monitoring systems, such as Prometheus. See [this presentation](https://schd.ws/hosted_files/cloudnativeeu2017/73/Integrating%20Long-Term%20Storage%20with%20Prometheus%20-%20CloudNativeCon%20Berlin%2C%20March%2030%2C%202017.pdf) for more on long term storage in Prometheus.

## M3DB

M3DB is a distributed time series database that provides scalable storage and a reverse index of time series. It is optimized as a cost effective and reliable realtime and long term retention metrics store and index.  For more details, see the [M3DB documentation](/v0.15.17/docs/m3db/).

## M3 Query

M3 Query is a service that houses a distributed query engine for querying both realtime and historical metrics, supporting several different query languages. It is designed to support both low latency realtime queries and queries that can take longer to execute, aggregating over much larger datasets, for analytical use cases.  For more details, see the [query engine documentation](/v0.15.17/docs/m3query/).

## M3 Aggregator

M3 Aggregator is a service that runs as a dedicated metrics aggregator and provides stream based downsampling, based on dynamic rules stored in etcd. It uses leader election and aggregation window tracking, leveraging etcd to manage this state, to reliably emit at-least-once aggregations for downsampled metrics to long term storage. This provides cost effective and reliable downsampling & roll up of metrics. These features also reside in the M3 Coordinator, however the dedicated aggregator is sharded and replicated, whereas the M3 Coordinator is not and requires care to deploy and run in a highly available way. There is work remaining to make the aggregator more accessible to users without requiring them to write their own compatible producer and consumer.
