---
linktitle: "Kubernetes"
title: Creating an M3 Cluster with Kubernetes
weight: 1
---

This guide shows you how to create an M3 cluster of 3 nodes, designed to run locally on the same machine. It is designed to show you how M3 and Kubernetes can work together, but not as a production example.

{{% notice note %}}
This guide assumes you have read the [quickstart](/docs/quickstart/docker), and builds upon the concepts in that guide.
{{% /notice %}}

{{% notice tip %}}
We recommend you use [our Kubernetes operator](/docs/operator/operator) to deploy M3 to a cluster. It is a more streamlined setup that uses [custom resource definitions](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) to automatically handle operations such as managing cluster placements.
{{% /notice %}}

{{< fileinclude file="cluster-architecture.md" >}}

## Prerequisites

-   A running Kubernetes cluster.
    -   For local testing, you can use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), [Docker desktop](https://www.docker.com/products/docker-desktop), or [we have a script](https://raw.githubusercontent.com/m3db/m3db-operator/master/scripts/kind-create-cluster.sh) you can use to start a 3 node cluster with [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/).

{{% notice note %}}
The rest of this guide uses minikube, you may need to change some of the steps to suit your local cluster.
{{% /notice %}}

## Create an etcd Cluster

M3 stores its cluster placements and runtime metadata in [etcd](https://etcd.io) and needs a running cluster to communicate with.

We have example services and stateful sets you can use, but feel free to use your own configuration and change any later instructions accordingly.


```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/{{% operator-version %}}/example/etcd/etcd-basic.yaml
```

{{% notice tip %}}
Depending on what you use to run a cluster on your local machine, you may need to update your _/etc/hosts_ file to match the domains specified in the `etcd` `--initial-cluster` argument. For example to match the `StatefulSet` declaration in the _etcd-minikube.yaml_ above, these are `etcd-0.etcd`, `etcd-1.etcd`, and `etcd-2.etcd`.
{{% /notice %}}

Verify that the cluster is running with something like the Kubernetes dashboard, or the command below:

```shell
kubectl exec etcd-0 -- env ETCDCTL_API=3 etcdctl endpoint health
```

## Install the Operator

Install the bundled operator manifests in the current namespace:

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/{{% operator-version %}}/bundle.yaml
```

## Create an M3 Cluster

The following creates an M3 cluster with 3 replicas of data across 256 shards that connects to the 3 available etcd endpoints.

It creates three isolated groups for nodes, each with one node instance. In a production environment you can use a variety of different options to define how nodes are spread across groups based on factors such as resource capacity, or location.

It creates namespaces in the cluster with the `namespaces` parameter. You can use M3-provided presets, or define your own. This example creates a namespace with the `10s:2d` preset.

The cluster derives pod identity from the `podIdentityConfig` parameter, which in this case is the UID of the Pod.

[Read more details on all the parameters in the Operator API docs](https://operator.m3db.io/api/).

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/{{% operator-version %}}/example/m3db-local.yaml
```

Verify that the cluster is running with something like the Kubernetes dashboard, or the command below:

```shell
kubectl exec simple-cluster-rep2-0 -- curl -sSf localhost:9002/health
```

## Deleting a Cluster

Delete the M3 cluster using kubectl:

```shell
kubectl delete m3dbcluster simple-cluster
```

By default, the operator uses [finalizers](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#finalizers) to delete the placement and namespaces associated with a cluster before the custom resources. If you do not want this behavior, set `keepEtcdDataOnDelete` to `true` in the cluster configuration.

<!-- TODO: Placement, same as Binaries? -->


## Organizing Data with Placements and Namespaces

A time series database (TSDBs) typically consist of one node (or instance) to store metrics data. This setup is simple to use but has issues with scalability over time as the quantity of metrics data written and read increases.

As a distributed TSDB, M3 helps solve this problem by spreading metrics data, and demand for that data, across multiple nodes in a cluster. M3 does this by splitting data into segments that match certain criteria (such as above a certain value) across nodes into shards.

<!-- TODO: Find an image -->

If you've worked with a distributed database before, then these concepts are probably familiar to you, but M3 uses different terminology to represent some concepts.

-   Every cluster has **one** placement that maps shards to nodes in the cluster.
-   A cluster can have **0 or more** namespaces that are similar conceptually to tables in other databases, and each node serves every namespace for the shards it owns.

<!-- TODO: Image -->

For example, if the cluster placement states that node A owns shards 1, 2, and 3, then node A owns shards 1, 2, 3 for all configured namespaces in the cluster. Each namespace has its own configuration options, including a name and retention time for the data.

## Create a Placement and Namespace

This quickstart uses the _{{% apiendpoint %}}database/create_ endpoint that creates a namespace, and the placement if it doesn't already exist based on the `type` argument.

You can create [placements](/docs/operational_guide/placement_configuration/) and [namespaces](/docs/operational_guide/namespace_configuration/#advanced-hard-way) separately if you need more control over their settings.

In another terminal, use the following command.

{{< tabs name="create_placement_namespace" >}}
{{< tab name="Command" >}}

{{< codeinclude file="docs/includes/create-database.sh" language="shell" >}}

{{< /tab >}}
{{% tab name="Output" %}}

```json
{
  "namespace": {
    "registry": {
      "namespaces": {
        "default": {
          "bootstrapEnabled": true,
          "flushEnabled": true,
          "writesToCommitLog": true,
          "cleanupEnabled": true,
          "repairEnabled": false,
          "retentionOptions": {
            "retentionPeriodNanos": "43200000000000",
            "blockSizeNanos": "1800000000000",
            "bufferFutureNanos": "120000000000",
            "bufferPastNanos": "600000000000",
            "blockDataExpiry": true,
            "blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
            "futureRetentionPeriodNanos": "0"
          },
          "snapshotEnabled": true,
          "indexOptions": {
            "enabled": true,
            "blockSizeNanos": "1800000000000"
          },
          "schemaOptions": null,
          "coldWritesEnabled": false,
          "runtimeOptions": null
        }
      }
    }
  },
  "placement": {
    "placement": {
      "instances": {
        "m3db_local": {
          "id": "m3db_local",
          "isolationGroup": "local",
          "zone": "embedded",
          "weight": 1,
          "endpoint": "127.0.0.1:9000",
          "shards": [
            {
              "id": 0,
              "state": "INITIALIZING",
              "sourceId": "",
              "cutoverNanos": "0",
              "cutoffNanos": "0"
            },
            …
            {
              "id": 63,
              "state": "INITIALIZING",
              "sourceId": "",
              "cutoverNanos": "0",
              "cutoffNanos": "0"
            }
          ],
          "shardSetId": 0,
          "hostname": "localhost",
          "port": 9000,
          "metadata": {
            "debugPort": 0
          }
        }
      },
      "replicaFactor": 1,
      "numShards": 64,
      "isSharded": true,
      "cutoverTime": "0",
      "isMirrored": false,
      "maxShardSetId": 0
    },
    "version": 0
  }
}
```

{{% /tab %}}
{{< /tabs >}}

Placement initialization can take a minute or two. Once all the shards have the `AVAILABLE` state, the node has finished bootstrapping, and you should see the following messages in the node console output.

<!-- TODO: Fix these timestamps -->

```shell
{"level":"info","ts":1598367624.0117292,"msg":"bootstrap marking all shards as bootstrapped","namespace":"default","namespace":"default","numShards":64}
{"level":"info","ts":1598367624.0301404,"msg":"bootstrap index with bootstrapped index segments","namespace":"default","numIndexBlocks":0}
{"level":"info","ts":1598367624.0301914,"msg":"bootstrap success","numShards":64,"bootstrapDuration":0.049208827}
{"level":"info","ts":1598367624.03023,"msg":"bootstrapped"}
```

You can check on the status by calling the _{{% apiendpoint %}}services/m3db/placement_ endpoint:

{{< tabs name="check_placement" >}}
{{% tab name="Command" %}}

```shell
curl {{% apiendpoint %}}services/m3db/placement | jq .
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "placement": {
    "instances": {
      "m3db_local": {
        "id": "m3db_local",
        "isolationGroup": "local",
        "zone": "embedded",
        "weight": 1,
        "endpoint": "127.0.0.1:9000",
        "shards": [
          {
            "id": 0,
            "state": "AVAILABLE",
            "sourceId": "",
            "cutoverNanos": "0",
            "cutoffNanos": "0"
          },
          …
          {
            "id": 63,
            "state": "AVAILABLE",
            "sourceId": "",
            "cutoverNanos": "0",
            "cutoffNanos": "0"
          }
        ],
        "shardSetId": 0,
        "hostname": "localhost",
        "port": 9000,
        "metadata": {
          "debugPort": 0
        }
      }
    },
    "replicaFactor": 1,
    "numShards": 64,
    "isSharded": true,
    "cutoverTime": "0",
    "isMirrored": false,
    "maxShardSetId": 0
  },
  "version": 2
}
```

{{% /tab %}}
{{< /tabs >}}

{{% notice tip %}}
[Read more about the bootstrapping process](/docs/operational_guide/bootstrapping_crash_recovery/).
{{% /notice %}}

{{< fileinclude file="cluster-common-steps.md" >}}
