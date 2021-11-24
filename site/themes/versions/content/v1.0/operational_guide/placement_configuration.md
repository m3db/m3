---
title: "Placement Configuration"
weight: 7
---

M3DB was designed from the ground up to be a distributed (clustered) database that is availability zone or rack aware (by using isolation groups). Clusters will seamlessly scale with your data, and you can start with a small number of nodes and grow it to a size of several hundred nodes with no downtime or expensive migrations.

Before reading the rest of this document, we recommend familiarizing yourself with the [M3DB placement documentation](/v1.0/docs/operational_guide/placement).

**Note**: The primary limiting factor for the maximum size of an M3DB cluster is the number of shards. Picking an appropriate number of shards is more of an art than a science, but our recommendation is as follows:

The number of shards that M3DB uses is configurable and there are a couple of key points to note when deciding the number to use. The
more nodes you have, the more shards you want because you want the shards to be evenly distributed amongst your nodes. However,
because each shard requires more files to be created, you also don’t want to have too many shards per node. This is due to the fact each
bit of data needs to be repartitioned and moved around the cluster (i.e. every bit of data needs to be moved all at once). Below are
some guidelines depending on how many nodes you will have in your cluster eventually - you will need to decide the number of shards up front, you
cannot change this once the cluster is created.

| Number of Nodes | Number of Shards |
| --------------- | ---------------- |
| 3               | 64               |
| 6               | 128              |
| 12              | 256              |
| 24              | 512              |
| 48              | 1024             |
| 128+            | 4096             |

After performing any of the instructions documented below a new placement will automatically be generated to distribute the shards among the M3DB nodes such that the isolation group and replication factor constraints are met.

If the constraints cannot be met, because there are not enough nodes to calculate a new placement such that each shard is replicated on the desired number of nodes with none of the nodes owning the same shard existing in the same isolation group, then the operation will fail.

In other words, all you have to do is issue the desired instruction and the M3 stack will take care of making sure that your data is distributed with appropriate replication and isolation.

In the case of the M3DB nodes, nodes that have received new shards will immediately begin receiving writes (but not serving reads) for the new shards that they are responsible for. They will also begin streaming in all the data for their newly acquired shards from the peers that already have data for those shards. Once the nodes have finished streaming in the data for the shards that they have acquired, they will mark their status for those shards as `Available` in the placement and begin accepting writes. Simultaneously, the nodes that are losing ownership of any shards will mark their status for those shards as `Leaving`. Once all the nodes accepting ownership of the new shards have finished streaming data from them, they will relinquish ownership of those shards and remove all the data associated with the shards they lost from memory and from disk.

M3Coordinator nodes will also pickup the new placement from etcd and alter which M3DB nodes they issue writes and reads to appropriately.

### Understanding the Placement Configuration

The placement configuration contains a few core values that control how the placement behaves.

#### ID

This is the identifier for a node in the placement and can be any value that uniquely identifies an M3DB node.

#### Isolation Group

This value controls how nodes that own the same M3DB shards are isolated from each other. For example, in a single datacenter configuration this value could be set to the rack that the M3DB node lives on. As a result, the placement will guarantee that nodes that exist on the same rack do not share any shards, allowing the cluster to survive the failure of an entire rack. Alternatively, if M3DB was deployed in an AWS region, the isolation group could be set to the region's availability zone and that would ensure that the cluster would survive the loss of an entire availability zone.

#### Zone

This value controls what etcd zone the M3DB node belongs to.

#### Weight

This value should be an integer and controls how the cluster will weigh the number of shards that an individual node will own. If you're running the M3DB cluster on homogenous hardware, then you probably want to assign all M3DB nodes the same weight so that shards are distributed evenly. On the otherhand, if you're running the cluster on heterogenous hardware, then this value should be higher for nodes with higher resources for whatever the limiting factor is in your cluster setup. For example, if disk space (as opposed to memory or CPU) is the limiting factor in how many shards any given node in your cluster can tolerate, then you could assign a higher value to nodes in your cluster that have larger disks and the placement calculations would assign them a higher number of shards.

#### Endpoint

This value should be in the form of `<M3DB_HOST_NAME>:<M3DB_NODE_LISTEN_PORT>` and identifies how network requests should be routed to this particular node in the placement.

#### Hostname

This value should be in the form of `<M3DB_HOST_NAME>` and identifies the address / host name of the M3DB node.

#### Port

This value should be in the form of `<M3DB_NODE_LISTEN_PORT>` and identifies the port over which this M3DB node expects to receive traffic (defaults to 9000).

### Placement Operations

**NOTE**: If you find yourself performing operations on seed nodes, please refer to the seed node-specific sections
below before making changes.

The instructions below all contain sample curl commands, but you can always review the API documentation by navigating to

`http://<M3_COORDINATOR_HOST_NAME>:<CONFIGURED_PORT(default 7201)>/api/v1/openapi` or our [online API documentation](https://m3db.io/openapi/).

**Note**: The [peers bootstrapper](/v1.0/docs/operational_guide/bootstrapping_crash_recovery) must be configured on all nodes in the M3DB cluster for placement changes to work. The `peers` bootstrapper is enabled by default, so you only need to worry about this if you modified the default bootstrapping configuration

Additionally, the following headers can be used in the placement operations: 

{{% fileinclude "headers_placement_namespace.md" %}}

#### Placement Initialization

Send a POST request to the `/api/v1/services/m3db/placement/init` endpoint

```shell
curl -X POST localhost:7201/api/v1/services/m3db/placement/init -d '{
    "num_shards": <DESIRED_NUMBER_OF_SHARDS>,
    "replication_factor": <DESIRED_REPLICATION_FACTOR>(recommended 3),
    "instances": [
        {
            "id": "<NODE_1_ID>",
            "isolation_group": "<NODE_1_ISOLATION_GROUP>",
            "zone": "<ETCD_ZONE>",
            "weight": <NODE_WEIGHT>,
            "endpoint": "<NODE_1_HOST_NAME>:<NODE_1_PORT>",
            "hostname": "<NODE_1_HOST_NAME>",
            "port": <NODE_1_PORT>
        },
        {
            "id": "<NODE_2_ID>",
            "isolation_group": "<NODE_2_ISOLATION_GROUP>",
            "zone": "<ETCD_ZONE>",
            "weight": <NODE_WEIGHT>,
            "endpoint": "<NODE_2_HOST_NAME>:<NODE_2_PORT>",
            "hostname": "<NODE_2_HOST_NAME>",
            "port": <NODE_2_PORT>
        },
        {
            "id": "<NODE_3_ID>",
            "isolation_group": "<NODE_3_ISOLATION_GROUP>",
            "zone": "<ETCD_ZONE>",
            "weight": <NODE_WEIGHT>,
            "endpoint": "<NODE_3_HOST_NAME>:<NODE_3_PORT>",
            "hostname": "<NODE_3_HOST_NAME>",
            "port": <NODE_3_PORT>
        }
    ]
}'
```

#### Adding a Node

Send a POST request to the `/api/v1/services/m3db/placement` endpoint

```shell
curl -X POST <M3_COORDINATOR_HOST_NAME>:<M3_COORDINATOR_PORT(default 7201)>/api/v1/services/m3db/placement -d '{
  "instances": [
    {
      "id": "<NEW_NODE_ID>",
      "isolationGroup": "<NEW_NODE_ISOLATION_GROUP>",
      "zone": "<ETCD_ZONE>",
      "weight": <NODE_WEIGHT>,
      "endpoint": "<NEW_NODE_HOST_NAME>:<NEW_NODE_PORT>(default 9000)",
      "hostname": "<NEW_NODE_HOST_NAME>",
      "port": <NEW_NODE_PORT>
    }
  ]
}'
```

After sending the add command you will need to wait for the M3DB cluster to reach the new desired state. You'll know that this has been achieved when the placement shows that all shards for all hosts are in the `Available` state.

#### Removing a Node

Send a DELETE request to the `/api/v1/services/m3db/placement/<NODE_ID>` endpoint.

```shell
curl -X DELETE <M3_COORDINATOR_HOST_NAME>:<M3_COORDINATOR_PORT(default 7201)>/api/v1/services/m3db/placement/<NODE_ID>
```

After sending the delete command you will need to wait for the M3DB cluster to reach the new desired state. You'll know that this has been achieved when the placement shows that all shards for all hosts are in the `Available` state.

#### Adding / Removing Seed Nodes

If you find yourself adding or removing etcd seed nodes then we highly recommend setting up an [external etcd](/v1.0/docs/operational_guide/etcd) cluster, as
the overhead of operating two stateful systems at once is non-trivial. As this is not a recommended production setup,
this section is intentionally brief.

To add or remove nodes to the etcd cluster, use `etcdctl member add` and `etcdctl member remove` as found in `Replacing
a Seed Node` below. A general rule to keep in mind is that any time the M3DB process starts on a seed node, the list of
cluster members in `etcdctl member list` must match _exactly_ the list in config.

#### Replacing a Node

**NOTE**: If using embedded etcd and replacing a seed node, please read the section below.

Send a POST request to the `/api/v1/services/m3db/placement/replace` endpoint containing hosts to replace and candidates to replace it with.

```shell
curl -X POST <M3_COORDINATOR_HOST_NAME>:<M3_COORDINATOR_PORT(default 7201)>/api/v1/services/m3db/placement/replace -d '{
    "leavingInstanceIDs": ["<OLD_NODE_ID>"],
    "candidates": [
        {
          "id": "<NEW_NODE_ID>",
          "isolationGroup": "<NEW_NODE_ISOLATION_GROUP>",
          "zone": "<ETCD_ZONE>",
          "weight": <NODE_WEIGHT>,
          "endpoint": "<NEW_NODE_HOST_NAME>:<NEW_NODE_PORT>(default 9000)",
          "hostname": "<NEW_NODE_HOST_NAME>",
          "port": <NEW_NODE_PORT>
        }
    ]
}'
```

#### Replacing a Seed Node

If you are using the embedded etcd mode (which is only recommended for test purposes) and replacing a seed node then
there are a few more steps to be done, as you are essentially doing two replace operations at once (replacing an etcd
node _and_ and M3DB node). To perform these steps you will need the `etcdctl` binary (version 3.2 or later), which can
be downloaded from the [etcd releases](https://github.com/etcd-io/etcd/releases) page.

Many of the instructions here are mentioned in the [etcd operational
guide](https://etcd.io/docs/latest/op-guide/runtime-configuration/), which we recommend
reading for more context.

To provide some context for the commands below, let's assume your cluster was created with the below configuration, and
that you'd like to replace `host3` with a new host `host4`, which has IP address `1.1.1.4`:

```yaml
initialCluster:
  - hostID: host1
    endpoint: http://1.1.1.1:2380
  - hostID: host2
    endpoint: http://1.1.1.2:2380
  - hostID: host3
    endpoint: http://1.1.1.3:2380
```

1.  On an existing node in the cluster that is **not** the one you're removing, use `etcdctl` to remove `host3` from the
    cluster:

```shell
$ ETCDCTL_API=3 etcdctl member list
9d29673cf1328d1, started, host1, http://1.1.1.1:2380, http://1.1.1.1:2379
f14613b6c8a336b, started, host2, http://1.1.1.2:2380, http://1.1.1.2:2379
2fd477713daf243, started, host3, http://1.1.1.3:2380, http://1.1.1.3:2379  # <<< INSTANCE WE WANT TO REMOVE

$ ETCDCTL_API=3 etcdctl member remove 2fd477713daf243
Removed member 2fd477713daf243 from cluster
```

2.  From the same host, use `etcdctl` to add `host4` to the cluster:

```shell
$ ETCDCTL_API=3 etcdctl member add host4 --peer-urls http://1.1.1.4:2380
```

3.  **Before** starting M3DB on `host4`, modify the initial cluster list to indicate `host4` has a cluster state of
    `existing`. Note: if you had previously started M3DB on this host, you'll have to remove the `member` subdirectory in
    `$M3DB_DATA_DIR/etcd/`.

```yaml
initialCluster:
  - hostID: host1
    endpoint: http://1.1.1.1:2380
  - hostID: host2
    endpoint: http://1.1.1.2:2380
  - hostID: host4
    clusterState: existing
    endpoint: http://1.1.1.4:2380
```

4.  Start M3DB on `host4`.

5.  On all other seed nodes, update their `initialCluster` list to be exactly equal to the list on `host4` from step 3.
    Rolling restart the hosts one at a time, waiting until they indicate they are bootstrapped (indicated in the
    `/health`) endpoint before continuing to the next.

6.  Follow the steps from `Replacing a Seed Node` to replace `host3` with `host4` in the M3DB placement.

#### Setting a new placement (Not Recommended)

This endpoint is unsafe since it creates a brand new placement and therefore should be used with extreme caution.
Some use cases for using this endpoint include:

-   Changing IP addresses of nodes
-   Rebalancing shards

If the placement for `M3DB` needs to be recreated, the `/api/v1/services/m3db/placement/set` can be used to do so.
Please note, a placement already needs to exist to use this endpoint. If no placement exists, use the `Placement Initialization`
endpoint described above. Also, as mentioned above, this endpoint creates an entirely new placement therefore 
complete placement information needs to be passed into the body of the request. The recommended way to this
is to get the existing placement using `/api/v1/services/m3db/placement` and modify that (as the `placement` field) along 
with two additional fields -- `version` and `confirm`. Please see below for a full example:

```shell
curl -X POST localhost:7201/api/v1/services/m3db/placement/set -d '{
  "placement": {
    "num_shards": <DESIRED_NUMBER_OF_SHARDS>,
    "replication_factor": <DESIRED_REPLICATION_FACTOR>(recommended 3),
    "instances": [
        {
            "id": "<NODE_1_ID>",
            "isolation_group": "<NODE_1_ISOLATION_GROUP>",
            "zone": "<ETCD_ZONE>",
            "weight": <NODE_WEIGHT>,
            "endpoint": "<NODE_1_HOST_NAME>:<NODE_1_PORT>",
            "hostname": "<NODE_1_HOST_NAME>",
            "port": <NODE_1_PORT>
        },
        {
            "id": "<NODE_2_ID>",
            "isolation_group": "<NODE_2_ISOLATION_GROUP>",
            "zone": "<ETCD_ZONE>",
            "weight": <NODE_WEIGHT>,
            "endpoint": "<NODE_2_HOST_NAME>:<NODE_2_PORT>",
            "hostname": "<NODE_2_HOST_NAME>",
            "port": <NODE_2_PORT>
        },
        {
            "id": "<NODE_3_ID>",
            "isolation_group": "<NODE_3_ISOLATION_GROUP>",
            "zone": "<ETCD_ZONE>",
            "weight": <NODE_WEIGHT>,
            "endpoint": "<NODE_3_HOST_NAME>:<NODE_3_PORT>",
            "hostname": "<NODE_3_HOST_NAME>",
            "port": <NODE_3_PORT>
        }
      ]
    },
    "version": <version>,
    "confirm": <true/false>
}'
```

**Note:** The `set` endpoint can also be used to set the placements in `M3Aggregator` and `M3Coordinator` using the following endpoints, respectively:

```shell
/api/v1/m3aggregator/set
/api/v1/m3coordinator/set
```
