# Roadmap

## Q3-Q4 2019: M3QL support
* Complete support for the M3 Query Language (M3QL)

## Q1-Q2 2019: Graphite and Prometheus feature parity
* Full support for Prometheus functions
* Ingestion and querying support for Graphite
* Tracing support using [Jaeger](https://www.jaegertracing.io/)
* Basic cost accounting
* Performance improvements

## Q3-Q4 2018: Dashboards can directly interact with M3 Query or M3 Coordinator to get data from M3DB (Q3-Q4 2018)

* Write a PromQL parser.
* Write the current M3QL interfaces to conform to the common DAG structure.
* Suggest auto aggregation rules.
* Provide advanced query tracking to figure out bottlenecks.

## Q2 2018: M3 Query and optimizations for M3 Coordinator (Q2 2018)

* Dedicated query engine service, M3 Query.
* Support authentication and rate limiting.
* Cost accounting per query and memory management to prevent M3 Query and M3 Coordinator from going OOM.
* Push computation to storage nodes whenever possible.
* Execution state manager to keep track of running queries.
* Port the distributed computation to M3 Query service.

## Q1 2018: Launch M3 Coordinator as a bridge for the read/write path of M3DB into open source

### Early Q1 2018: V1
* Create a gRPC/Protobuf service.
* Handlers for Prometheus remote read/write endpoints.
* Perform fanout to M3DB nodes.
* Tooling to set up M3 Coordinator alongside Prometheus.

### Late Q1 2018: V2
* Support cross datacenter calls with remote aggregations.
* Benchmark the performance of the coordinator and M3DB using popular datasets.
* Support for multiple M3DB clusters.
