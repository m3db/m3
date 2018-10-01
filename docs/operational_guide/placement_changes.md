# Placement Changes

## Overview

M3DB was designed from the ground up to a be a distributed / clustered database that is isolation group aware. Clusters will seamlessly scale with your data, and you can start with a cluster as small as 3 nodes and grow it to a size of several hundred nodes with no downtime or expensive migrations.

Before reading the rest of this document, we recommend familiarizing yourself with the [M3DB placement documentation](placement.md)

**Note**: The primary limiting factor for the maximum size of an M3DB cluster is the number of shards.  TODO: Explain how to pick an appropriate number of shards and the tradeoff with a (small) linear increase in required node resources with the number of shards.

TODO: Explain that the peers bootstrapped MUST be enabled for placement changes to work.

After performing any of the instructions documented below a new placement will automatically be generated to distribute the shards among the M3DB nodes such that the isolation group and replication factor constraints are met.

If the constraints cannot be met, because there are not enough nodes to calculate a new placement such that each shard is replicated on the desired number of nodes with none of the nodes owning the same shard existing in the same isolation group, then the operation will fail.

In other words, all you have to do is issue the desired instruction and the M3 stack will take care of making sure that your data is distributed with appropriate replication and isolation.

In the case of the M3DB nodes, nodes that have received new shards will immediately begin receiving writes (but not serving reads) for the new shards that they are responsible for. They will also begin streaming in all the data for their newly acquired shards from the peers that already have data for those shards. Once the nodes have finished streaming in the data for the shards that they have acquired, they will mark their status for those shards as `Available` in the placement and begin accepting writes. Simultaneously, the nodes that are losing ownership of any shards will mark their status for those shards as `LEAVING`. Once all the nodes accepting ownership of the new shards have finished streaming data from them, they will relinquish ownership of those shards and remove all the data associated with the shards they lost from memory and from disk.

M3Coordinator nodes will also pickup the new placement from etcd and alter which M3DB nodes they issue writse and reads to appropriately.

### Understanding the Placement Configuration

The placement configuration contains a few core values that control how the placement behaves.

#### ID

This is the identifier for a node in the placement and can be any value that uniquely identifies an M3DB node.

#### Isolation Group

This value controls how nodes that own the same M3DB shards are isolated from each other. For example, in a single datacenter configuration this value could be set to the rack that the M3DB node lives on. As a result, the placement will guarantee that nodes that exist on the same rack do not share any shards, allowing the cluster to survive the failure of an entire rack. Alternatively, if M3DB was deployed in an AWS region, the isolation group could be set to the regions availability zone and that would ensure that the cluster would survive the loss of an entire availability zone.

#### Zone

This value controls what etcd zone the M3DB node belongs to.

#### Weight

This value should be an integer and controls how the cluster will weigh the number of shards that an individual node will own. If you're running the M3DB cluster on homogenous hardware, then you probably want to assign all M3DB nodes the same weight so that shards are distributed evenly. On the otherhand, if you're running the cluster on heterogenous hardware, then this value should be higher for nodes with higher resources for whatever the limiting factor is in your cluster setup. For example, if disk space (as opposed to memory or CPU) is the limiting factor in how many shards any given node in your cluster can tolerate, then you could assign a higher value to nodes in your cluster that have larger disks and the placement calcualtions would assign them a higher number of shards.

#### Endpoint

This value should be in the form of <M3DB_HOST_NAME>:<M3DB_NODE_LISTEN_PORT> and identifies how network requests should be routed to this particular node in the placement.

#### Hostname

This value should be in the form of <M3DB_HOST_NAME> and identifies the address / host name of the M3DB node.

#### Port

This value should be in the form of <M3DB_NODE_LISTEN_PORT> and identifies the port over which this M3DB node expects to receive traffic (defaults to 9000).

### Placement Operations

The instructions below all contain sample curl commands, but you can always review the API documentation by navigating to

`http://<M3_COORDINATOR_HOST_NAME>:<CONFIGURED_PORT(default 7201)>/api/v1/openapi`

#### Placement Initialization

Send a POST request to the `/api/v1/placement/init` endpoint

```bash
curl -X POST localhost:7201/api/v1/placement/init -d '{
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
        },
    ]
}'
```

#### Adding a Node

Send a POST request to the `/api/v1/placement` endpoint

```bash
curl -X POST <M3_COORDINATOR_HOST_NAME>:<M3_COORDINATOR_PORT(default 7201)>/api/v1/placement -d '{
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

#### Removing a Node

Send a DELETE request to the `/api/v1/placement/<NODE_ID>` endpoint.

```bash
curl -X DELETE <M3_COORDINATOR_HOST_NAME>:<M3_COORDINATOR_PORT(default 7201)>/api/v1/placement/<NODE_ID>
```

#### Replacing a Node
TODO


