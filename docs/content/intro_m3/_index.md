+++
title = "Introduction to M3"
date = 2020-04-22T15:05:49-04:00
weight = 1
chapter = true
pre = "<b>1. </b>"
+++

### Introduction to M3

### About
After using open-source metrics solutions and finding issues with them at scale, such as reliability, cost, and operational complexity, M3 was created from the ground up to provide Uber with a native, distributed time series database, a highly-dynamic and performant aggregation service, a query engine, and other supporting infrastructure.

### Key Features
M3 has several features, provided as discrete components, which make it an ideal platform for time series data at scale:

* A distributed time series database, M3DB, that provides scalable storage for time series data and a reverse index.
* A sidecar process, M3Coordinator, that allows M3DB to act as the long-term storage for Prometheus.
* A distributed query engine, M3Query, with native support for PromQL and Graphite (M3QL coming soon).
* An aggregation tier, M3Aggregator, that runs as a dedicated metrics aggregator/downsampler allowing metrics to be stored at various retentions at different resolutions.


## Components
### M3 Coordinator
M3 Coordinator is a service that coordinates reads and writes between upstream systems, such as Prometheus, and M3DB. It is a bridge that users can deploy to access the benefits of M3DB such as long term storage and multi-DC setup with other monitoring systems, such as Prometheus. See this presentation for more on long term storage in Prometheus.

### M3DB
M3DB is a distributed time series database that provides scalable storage and a reverse index of time series. It is optimized as a cost effective and reliable realtime and long term retention metrics store and index. For more details, see the M3DB documentation.

### M3 Query
M3 Query is a service that houses a distributed query engine for querying both realtime and historical metrics, supporting several different query languages. It is designed to support both low latency realtime queries and queries that can take longer to execute, aggregating over much larger datasets, for analytical use cases. For more details, see the query engine documentation.

### M3 Aggregator
M3 Aggregator is a service that runs as a dedicated metrics aggregator and provides stream based downsampling, based on dynamic rules stored in etcd. It uses leader election and aggregation window tracking, leveraging etcd to manage this state, to reliably emit at-least-once aggregations for downsampled metrics to long term storage. This provides cost effective and reliable downsampling & roll up of metrics. These features also reside in the M3 Coordinator, however the dedicated aggregator is sharded and replicated, whereas the M3 Coordinator is not and requires care to deploy and run in a highly available way. There is work remaining to make the aggregator more accessible to users without requiring them to write their own compatible producer and consumer.

## Motivation
We decided to open source the M3 platform as a scalable remote storage backend for Prometheus and Graphite so that others may attempt to reuse our work and avoid building yet another scalable metrics platform. As documentation for Prometheus states, it is limited by single nodes in its scalability and durability. The M3 platform aims to provide a turnkey, scalable, and configurable multi-tenant store for Prometheus, Graphite and other standard metrics schemas.
