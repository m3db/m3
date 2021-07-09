## M3 Architecture

<!-- TODO: Update image -->

![Typical Deployment](/cluster_architecture.png)

### Node types

An M3 deployment typically has two main node types:

- **[Storage nodes](/v1.0/docs/reference/m3db)** (`m3dbnode`) are the workhorses of M3, they store data and serve reads and writes.
- **[Coordinator nodes](/v1.0/docs/reference/m3coordinator)** (`m3coordinator`) coordinate reads and writes across all nodes in the cluster. It's a lightweight process, and does not store any data. This role typically runs alongside a Prometheus instance, or is part of a collector agent such as statsD.

A `m3coordinator` node exposes two external ports:

-   `7201` to manage the cluster topology, you make most API calls to this endpoint
-   `7203` for Prometheus to scrape the metrics produced by M3DB and M3Coordinator

There are two other less-commonly used node types:

- **[Query nodes](/v1.0/docs/reference/m3query)** (`m3query`) are an alternative query option to using M3's built-in PromQL support.
- **[Aggregator nodes](/v1.0/docs/how_to/m3aggregator)** cluster and aggregate metrics before storing them in storage nodes. Coordinator nodes can also perform this role but are not cluster-aware.

<!-- TODO: Add more about what's bundled -->

## Prerequisites

M3 uses [etcd](https://etcd.io/) as a distributed key-value storage for the following functions:

-   Update cluster configuration in realtime
-   Manage placements for distributed and sharded clusters