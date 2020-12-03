---
linktitle: "Docker"
weight: 3
draft: true
---

# Creating an M3 Cluster with Docker

This guide shows you the steps involved in creating an M3 cluster using Docker containers, typically you would automate this with infrastructure as code tools such as Terraform or Kubernetes.

{{% notice note %}}
This guide assumes you have read the [quickstart](/docs/quickstart), and builds upon the concepts in that guide.
{{% /notice %}}

## M3 Architecture

Here's a typical M3 deployment:

<!-- TODO: Update image -->

![Typical Deployment](/cluster_architecture.png)

An M3 deployment typically has two main node types:

-   **Coordinator node**: `m3coordinator` nodes coordinate reads and writes across all nodes in the cluster. It's a lightweight process, and does not store any data. This role typically runs alongside a Prometheus instance, or is part of a collector agent.
-   **Storage node**: The `m3dbnode` processes are the workhorses of M3, they store data and serve reads and writes.

And exposes two ports:

-   `7201` to manage the cluster topology, you make most API calls to this endpoint
-   `7203` for Prometheus to scrape the metrics produced by M3DB and M3Coordinator

## Prerequisites

M3 uses [etcd](https://etcd.io/) as a distributed key-value storage for the following functions:

-   Update cluster configuration in realtime
-   Manage placements for distributed and sharded clusters

{{% notice note %}}
M3 storage nodes have an embedded etcd server you can use for small test clusters which we call a **Seed Node** when run this way. See the `etcdClusters` section of [this example configuration file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd.yml).
{{% /notice %}}

## Download and Install a Binary

You can download the latest release as [pre-compiled binaries from the M3 GitHub page](https://github.com/m3db/m3/releases/latest). Inside the expanded archive are binaries for `m3dbnode`, which combines a coordinator and storage node, and a binary for `m3coordinator`, which is a standalone coordinator node.
