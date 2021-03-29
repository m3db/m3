---
linktitle: "Binaries"
title: Creating a Single Node M3 Cluster with Binaries
weight: 3
---

This guide shows how to install and configure M3, create a single-node cluster, and read and write metrics to it.

{{% notice warning %}}
Deploying a single-node M3 cluster is a great way to experiment with M3 and get an idea of what it has to offer, but is not designed for production use. To run M3 in clustered mode, with a separate M3Coordinator [read the clustered mode guide](/docs/cluster).
{{% /notice %}}

## Prebuilt Binaries

M3 has pre-built binaries available for Linux and macOS. [Download the latest release from GitHub](https://github.com/m3db/m3/releases/latest).

## Build From Source

### Prerequisites

-   [Go 1.10 or higher](https://golang.org/dl/)
-   [Make](https://www.gnu.org/software/make/)

### Build Source

```shell
make m3dbnode
```

## Start Binary

By default the binary configures a single M3 instance containing:

-   An M3DB storage instance for time series storage. It includes an embedded tag-based metrics index and an etcd server for storing the cluster topology and runtime configuration.
-   An M3Coordinator instance for writing and querying tagged metrics, as well as managing cluster topology and runtime configuration.

It exposes three ports:

-   `7201` to manage the cluster topology, you make most API calls to this endpoint
-   `7203` for Prometheus to scrape the metrics produced by M3DB and M3Coordinator

The command below starts the node using the specified configuration file.

{{< tabs name="start_container" >}}
{{% tab name="Pre-built binary" %}}

[Download the example configuration file](https://github.com/m3db/m3/raw/master/src/dbnode/config/m3dbnode-local-etcd.yml).

```shell
./m3dbnode -f /{FILE_LOCATION}/m3dbnode-local-etcd.yml
```

{{% notice info %}}
Depending on your operating system setup, you might need to prefix the command with `sudo`.
{{% /notice %}}

{{% /tab %}}
{{% tab name="Self-built binary" %}}

```shell
./bin/m3dbnode -f ./src/dbnode/config/m3dbnode-local-etcd.yml
```

{{% notice info %}}
Depending on your operating system setup, you might need to prefix the command with `sudo`.
{{% /notice %}}

{{% /tab %}}
{{% tab name="Output" %}}

<!-- TODO: Perfect image, pref with terminalizer -->

<!-- TODO: Update image -->

![Docker pull and run](/docker-install.gif)

{{% /tab %}}
{{< /tabs >}}

{{% notice info %}}
When running the command above on macOS you may see errors about "too many open files." To fix this in your current terminal, use `ulimit` to increase the upper limit, for example `ulimit -n 10240`.
{{% /notice %}}

## Configuration

This example uses this [sample configuration file](https://github.com/m3db/m3/raw/master/src/dbnode/config/m3dbnode-local-etcd.yml) by default.

The file groups configuration into `coordinator` or `db` sections that represent the `M3Coordinator` and `M3DB` instances of single-node cluster.

{{% notice tip %}}
You can find more information on configuring M3DB in the [operational guides section](/docs/operational_guide).
{{% /notice %}}

{{< fileinclude file="quickstart-common-steps.md" >}}