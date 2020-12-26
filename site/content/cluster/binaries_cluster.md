---
linktitle: "Binaries"
title: Creating an M3 Cluster with Binaries
weight: 2
---

This guide shows you the steps involved in creating an M3 cluster using M3 binaries, typically you would automate this with infrastructure as code tools such as Terraform or [Kubernetes](/docs/operator).

{{% notice note %}}
This guide assumes you have read the [quickstart](/docs/quickstart/binaries), and builds upon the concepts in that guide.
{{% /notice %}}

## M3 Architecture

Here's a typical M3 deployment:

<!-- TODO: Update image -->

![Typical Deployment](/cluster_architecture.png)

An M3 deployment typically has two main node types:

-   **Coordinator node**: `m3coordinator` nodes coordinate reads and writes across all nodes in the cluster. It's a lightweight process, and does not store any data. This role typically runs alongside a Prometheus instance, or is part of a collector agent such as statsD.
-   **Storage node**: The `m3dbnode` processes are the workhorses of M3, they store data and serve reads and writes.

A `m3coordinator` node exposes two ports:

-   `7201` to manage the cluster topology, you make most API calls to this endpoint
-   `7203` for Prometheus to scrape the metrics produced by M3DB and M3Coordinator

## Prerequisites

M3 uses [etcd](https://etcd.io/) as a distributed key-value storage for the following functions:

-   Update cluster configuration in realtime
-   Manage placements for distributed and sharded clusters

## Download and Install a Binary

You can download the latest release as [pre-compiled binaries from the M3 GitHub page](https://github.com/m3db/m3/releases/latest). Inside the expanded archive are binaries for `m3dbnode`, which combines a coordinator and storage node, and a binary for `m3coordinator`, which is a standalone coordinator node.

## Build from Source

### Prerequisites

-   [Go](https://golang.org/dl/)
-   [Make](https://www.gnu.org/software/make/)

### Build

[Clone the codebase](https://github.com/m3db/m3) and run `make m3dbnode` to generate a binary for a combination coordinator and storage node, or `make m3coordinator` to generate a binary for a standalone coordinator node.

## Provision a Host

Enough background, let's create a real cluster!

M3 in production can run on local or cloud-based VMs, or bare-metal servers. M3 supports all popular Linux distributions (Ubuntu, RHEL, CentOS), and [let us know](https://github.com/m3db/m3/issues/new/choose) if you have any issues with your preferred distribution.

### Network

{{% notice tip %}}
If you use AWS or GCP, we recommend you use static IPs so that if you need to replace a host, you don't have to update configuration files on all the hosts, but decommission the old seed node and provision a new seed node with the same host ID and static IP that the old seed node had. If you're using AWS you can use an [Elastic Network Interface](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html) on a Virtual Private Cloud (VPC) and for GCP you can use an [internal static IP address](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-internal-ip-address).
{{% /notice %}}

This example creates three static IP addresses for three storage nodes, using the embedded coordinator.

This guide assumes you have host names configured, i.e., running `hostname` on a host in the cluster returns the host ID you use when creating the cluster placement.

{{% notice tip %}}
When using GCP the name of your instance is the host name. When you create an instance, click _Management, disks, networking, SSH keys_, under _Networking_, click the default interface, click the _Primary internal IP_ drop down, select _Reserve a static internal IP address_, give it an appropriate name and description, and use _Assign automatically_.
{{% /notice %}}

{{% notice tip %}}
When using AWS, you can use the host name supplied for the provisioned VM as your host ID, or use the `environment` host ID resolver and pass the host ID when launching the database process with an environment variable.
{{% /notice %}}

For example, if you used `M3DB_HOST_ID` for the environment variable name, use the following in your configuration:

```yaml
hostID:
  resolver: config
  value: ${M3DB_HOST_ID:""}
```

Then start the `m3dbnode` process with:

{{< tabs name="start_container" >}}
{{% tab name="Pre-built binary" %}}

```shell
M3DB_HOST_ID=m3db001 m3dbnode -f <config-file.yml>
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

### Kernel Configuration

Depending on the default limits of your bare-metal machine or VM, M3 may need some Kernel tweaks to run as efficiently as possible, and [we recommend you review those](/docs/operational_guide/kernel_configuration) before running M3 in production.

## Configuration files

You configure each M3 component by passing the location of a YAML file with the `-f` argument.

The file groups configuration into `coordinator` or `db` sections that represent the `M3Coordinator` and `M3DB` instances of single-node cluster.

{{% notice tip %}}
You can find more information on configuring M3DB in the [operational guides section](/docs/operational_guide/).
{{% /notice %}}

{{% notice note %}}
The steps in this guide have the following 3 seed nodes, you need to change your configuration to suit the details of yours, including the details of an etcd cluster in the `m3dbCluster` > `endpoints` section of the M3 configuration file.
{{% /notice %}}

-   m3db001 (Region=us-east1, Zone=us-east1-a, Static IP=10.142.0.1)
-   m3db002 (Region=us-east1, Zone=us-east1-b, Static IP=10.142.0.2)
-   m3db003 (Region=us-east1, Zone=us-east1-c, Static IP=10.142.0.3)

### M3DB node

[Start with the M3DB configuration template](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-cluster-template.yml) and change it to suit your cluster.

The example below connects to an etcd instance in a zone called `eu-1`
This example updates the `service` and `seedNodes` sections to match the node details above:

<!-- TODO: Add more details on config items here -->

```yaml
config:
  discovery: 
    type: m3db_cluster 
    m3dbCluster: 
      env: default_env
      endpoints:
        - 10.142.0.1:2379
        - 10.142.0.2:2379
        - 10.142.0.3:2379
```

## Start the storage nodes

Start each seed node in the cluster using the same configuration file, and adjusting the `M3DB_HOST_ID` value to match the host name.

```shell
M3DB_HOST_ID=m3db001 m3dbnode -f <config-file.yml>
```

{{% notice tip %}}
You can daemon-ize the node startup process using your favorite utility such as systemd, init.d, or supervisor.
{{% /notice %}}

## Create Namespace and Initialize Placement

<!-- TODO: Again partialise and include across guides when complete -->

<!-- TODO: Probably take this example up a notch for the how to guides -->

This guide uses the _{{% apiendpoint %}}database/create_ endpoint that creates a namespace, and the placement if it doesn't already exist based on the `type` argument.

You can create [placements](/docs/operational_guide/placement_configuration/) and [namespaces](/docs/operational_guide/namespace_configuration/#advanced-hard-way) separately if you need more control over their settings.

In the example below, the configuration for each host matches the details outlined above for the three nodes used. `isolationGroup` specifies how the cluster places shards to avoid more than one replica of a shard appearing in the same replica group. You should use at least as many isolation groups as your replication factor. This example uses the availability zones `us-east1-a`, `us-east1-b`, `us-east1-c` as the isolation groups which matches our replication factor of 3. [Read more details in this guide](/docs/operational_guide/replication_and_deployment_in_zones).

{{< tabs name="database_create" >}}
{{% tab name="Command" %}}

<!-- TODO: create partial -->

```shell
curl -X POST {{% apiendpoint %}}database/create -d '{
  "type": "cluster",
  "namespaceName": "default",
  "retentionTime": "48h",
  "numShards": "1024",
  "replicationFactor": "3",
  "hosts": [
        {
            "id": "m3db001",
            "isolationGroup": "us-east1-a",
            "zone": "embedded",
            "weight": 100,
            "address": "10.142.0.1",
            "port": 9000
        },
        {
            "id": "m3db002",
            "isolationGroup": "us-east1-b",
            "zone": "embedded",
            "weight": 100,
            "address": "10.142.0.2",
            "port": 9000
        },
        {
            "id": "m3db003",
            "isolationGroup": "us-east1-c",
            "zone": "embedded",
            "weight": 100,
            "address": "10.142.0.3",
            "port": 9000
        }
    ]
}'
```

{{% /tab %}}
{{% tab name="Output" %}}

```shell
20:10:12.911218[I] updating database namespaces [{adds [default]} {updates []} {removals []}]
20:10:13.462798[I] node tchannelthrift: listening on 0.0.0.0:9000
20:10:13.463107[I] cluster tchannelthrift: listening on 0.0.0.0:9001
20:10:13.747173[I] node httpjson: listening on 0.0.0.0:9002
20:10:13.747506[I] cluster httpjson: listening on 0.0.0.0:9003
20:10:13.747763[I] bootstrapping shards for range starting ...
...
20:10:13.757834[I] bootstrap finished [{namespace metrics} {duration 10.1261ms}]
20:10:13.758001[I] bootstrapped
20:10:14.764771[I] successfully updated topology to 3 hosts
```

{{% /tab %}}
{{< /tabs >}}

If you need to setup multiple namespaces, you can run the command above multiple times with different namespace configurations.

### Ready a Namespace
<!-- TODO: Why?> -->
Once a namespace has finished bootstrapping, you must mark it as ready before receiving traffic by using the _{{% apiendpoint %}}namespace/ready_.

{{< tabs name="ready_namespaces" >}}
{{% tab name="Command" %}}

{{< codeinclude file="docs/includes/quickstart/ready-namespace.sh" language="shell" >}}

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "ready": true
}
```

{{% /tab %}}
{{< /tabs >}}

### Replication factor

We recommend a replication factor of **3**, with each replica spread across failure domains such as a physical server rack, data center or availability zone. Read our [replication factor recommendations](/docs/operational_guide/replication_and_deployment_in_zones) for more details.

### Shards

Read the [placement configuration guide](/docs/operational_guide/placement_configuration) to determine the appropriate number of shards to specify.

{{< fileinclude file="cluster-common-steps.md" >}}

<!-- ## Next Steps

This quickstart covered getting a single-node M3DB cluster running, and writing and querying metrics to the cluster. Some next steps are:

-   one
-   two -->
