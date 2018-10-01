# Placement Changes

## Overview

M3DB was designed from the ground up to a be a distributed / clustered database that is isolation group aware. Clusters can easily grow with your data, and you can start with a cluster as small as 3 nodes and grow it to a size of several hundred nodes with no downtime or expensive migrations.

Before reading the rest of this document, we recommend familiarizing yourself with the [M3DB placement documentation](placement.md)

**Note**: The primary limiting factor for the maximum size of an M3DB cluster is the number of shards.  TODO: Explain how to pick an appropriate number of shards and the tradeoff with a (small) linear increase in required node resources with the number of shards.

TODO: Explain that the peers bootstrapped MUST be enabled for placement changes to work.

After performing any of the instructions documented below a new placement will automatically be generated to distribute the shards among the M3DB nodes such that the isolation group and replication factor constraints are met.

If the constraints cannot be met in the situation that there are not enough nodes to achieve the desired replication factor with each shards replicated on the desired number of nodes with none of the nodes owning the same shard existing in the same isolation group, then the operation will fail.

In other words, all you have to do is issue the desired instruction and the M3 stack will take care of making sure that your data is distributed with appropriate replication and isolation.

After issuing any of the command below, all the M3DB nodes (as well as any M3Coordinator intances) will automatically pickup the new placement from etcd and perform all the necessary work to reshard the data automatically.

In the case of the M3DB nodes, nodes that have received new shards will immediately begin receiving writes (but not serving reads) for the new shards that they are responsible for. They will also begin streaming in all the data for their newly acquired shards from the peers that already have data for those shards. Once the nodes have finished streaming in the data for the shards that they have acquired, they will mark their status for those shards as `Available` in the placement and begin accepting writes. Simultaneously, the nodes that are losing ownership of any shards will mark their status for those shards as `LEAVING`. Once all the nodes acecpting ownership of the new shards have finished streaming data from them, they will relinquish ownership of those shards and remove all the data associated with the shards they lost from memory and from disk.

M3Coordinator nodes will also pickup the new placement from etcd and alter which M3DB nodes they issue writse and reads to appropriately.

### Understanding the Placement Configuration

The placement configuration contains a few core values that control how the placement behaves.

#### ID

This is the identifier for a node in the placement and can be any value that uniquely identifers an M3DB node.

#### Isolation Group

This value controls how nodes that own the same M3DB shards are isolated from each other. For example, in a single datacenter configuration this value could be set to the rack that the M3DB node lives on. As a result, the placement will guarantee that nodes that exist on the same rack do not share any shards, allowing the cluster to survive the failure of an entire rack.

#### Zone

This value controls what etcd zone the M3DB node belongs to.

#### Weight

This value should be an integer and controls how the cluster will weigh the number of shards that an individual node will own. If you're running the M3DB cluster on homogenous hardware, then you should assign all M3DB nodes the same weight. On the otherhand, if you're running the cluster on heterogenous hardware, then this value should be higher for nodes with higher resources for whatever the limiting factor is in your cluster setup. For example, if disk space (as opposed to memory or CPU) was the limiting factor in how many shards any given node in your cluster can tolerate,then you could assign a higher value to nodes in your cluster that have larger disks and the placement would assign them a higher number of shards.

#### Endpoint

This value should be in the form of <M3DB_NODE_ADDRESS>:<M3DB_NODE_PORT> and identifies how network requests should be routed to this particular node in the placement.

#### Hostname

This value should be in the form of <M3DB_NODE_ADDRESS> and identifies the address of the M3DB node.

#### Port

This value should be in the form of <M3DB_NODE_LISTEN_PORT> and identifies the port over which this M3DB node expects to receive traffic (defaults to 9000).

### Placement Operations

The instructions below all contain sample curl commands, but you can always review the API documentation at TODO INSTRUCTIONS TO VIEW API DOCS.

#### Placement Initialization
TOOD

#### Adding a Node

All you have to do is send a POST request to the /api/v1/placement endpoint

```
curl -X POST <M3_COORDINATOR_IP_ADDRESS>:<M3_COORDINATOR_LISTEN_ADDRESS_PORT(default 7201)>/api/v1/placement -d '{
  "instances": [
    {
      "id": "<NEW_NODE_ID>",
      "isolationGroup": "<NEW_NODE_ISOLATION_GROUP>",
      "zone": "<ETCD_ZONE>",
      "weight": <NODE_WEIGHT>,
      "endpoint": "<NEW_NODE_ADDRESS>:<NEW_NODE_LISTEN_PORT>(default 9000)",
      "hostname": "<NEW_NODE_ADDRESS>",
      "port": <NEW_NODE_LISTEN_PORT>
    }
  ]
}'
```

#### Removing a Node

All you have to do is send a DELETE request to the /api/v1/placement/<NODE_ID> endpoint.

```
curl -X DELETE <M3_COORDINATOR_IP_ADDRESS>:<M3_COORDINATOR_LISTEN_ADDRESS_PORT(default 7201)>/api/v1/placement/<NODE_ID>
```

#### Replacing a Node
TODO


