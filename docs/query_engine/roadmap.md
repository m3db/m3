# Roadmap

## Launch M3 Coordinator as a bridge for the Read/Write path of M3DB into open source (Q1 2018) 

### V1 (late Q4 2017 - early Q1 2018)
* Create a gRPC/Protobuf service.
* Handlers for Prometheus remote read/write endpoints.
* Perform fanout to M3DB nodes.
* Tooling to set up M3 Coordinator alongside Prometheus.

### V2 (late Q1 2018)
* Support cross datacenter calls with remote aggregations.
* Benchmark the performance of the coordinator and M3DB using popular datasets.
* Support for multiple M3DB clusters.

## M3 Query and optimizations for M3 Coordinator (Q2 2018)

* Dedicated query engine service, M3 Query.
* Support authentication and rate limiting.
* Cost accounting per query and memory management to prevent M3 Query and M3 Coordinator from going OOM.
* Push computation to storage nodes whenever possible.
* Execution state manager to keep track of running queries.
* Port the distributed computation to M3 Query service.

## Dashboards can directly interact with M3 Query or M3 Coordinator to get data from M3DB (Q3-Q4 2018)

* Write a PromQL parser.
* Write the current M3QL interfaces to conform to the common DAG structure.
* Suggest auto aggregation rules.
* Provide advanced query tracking to figure out bottlenecks.


