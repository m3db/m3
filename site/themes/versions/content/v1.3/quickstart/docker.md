---
linkTitle: "Docker"
title: Creating a Single Node M3 Cluster with Docker
weight: 1
---

This guide shows how to install and configure M3, create a single-node cluster, and read and write metrics to it.

{{% notice warning %}}
Deploying a single-node cluster is a great way to experiment with M3 and get an idea of what it has to offer, but isn't designed for production use. To run M3 in clustered mode with a separate M3 Coordinator, [read the clustered mode guides](/docs/cluster/).
{{% /notice %}}

## Prerequisites

-   **Docker**: You don't need [Docker](https://www.docker.com/get-started) to run M3DB, but it's the simplest and quickest way.
    -   If you use Docker Desktop, we recommend the following minimum _Resources_ settings.
        -   _CPUs_: 2
        -   _Memory_: 8GB
        -   _Swap_: 1GB
        -   _Disk image size_: 16GB
-   **JQ**: This example uses [jq](https://stedolan.github.io/jq/) to format the output of API calls. It isn't essential for using M3DB.
-   **curl**: This example uses curl for communicating with M3DB endpoints. You can also use alternatives such as [Wget](https://www.gnu.org/software/wget/) and HTTPie.

## Start Docker Container

By default the official M3 Docker image configures a single instance as one binary containing:

-   An M3DB storage instance for time series storage. It includes an embedded tag-based metrics index and an etcd server for storing the cluster topology and runtime configuration.
-   A M3 Coordinator instance for writing and querying tagged metrics, as well as managing cluster topology and runtime configuration.

The Docker container exposes two ports:

-   `7201` to manage the cluster topology, you make most API calls to this endpoint
-   `7203` for Prometheus to scrape the metrics produced by M3DB and M3 Coordinator

The command below creates a persistent data directory on the host operating system to maintain durability and persistence between container restarts.

{{< tabs name="start_container" >}}
{{% tab name="Command" %}}

```shell
docker run -p 7201:7201 -p 7203:7203 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db quay.io/m3db/m3dbnode:{{% docker-version %}}
```

{{% /tab %}}
{{% tab name="Output" %}}

<!-- TODO: Perfect image, pref with terminalizer -->

![Docker pull and run](/docker-install.gif)

{{% /tab %}}
{{< /tabs >}}

{{% notice info %}}
When running the command above on Docker for Mac, Docker for Windows, and some Linux distributions you may see errors about settings not being at recommended values. Unless you intend to run M3 in production on macOS or Windows, you can ignore these warnings.
{{% /notice %}}

## Configuration

The single-node cluster Docker image uses this [sample configuration file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd.yml) by default.

The file groups configuration into `coordinator` or `db` sections that represent the `M3Coordinator` and `M3DB` instances of single-node cluster.

<!-- TODO: Replicate relevant sections -->

{{% notice tip %}}
You can find more information on configuring M3 in the [operational guides section](/docs/operational_guide/).
{{% /notice %}}

{{< fileinclude "docs/includes/quickstart-common-steps.md" >}}
