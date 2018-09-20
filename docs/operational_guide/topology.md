# Topology

## Overview

M3DB stores its topology (mapping of which hosts are responsible for which shards) in etcd. There are three possible states that each host/shard pair can be in:

1. Initializing
2. Available
3. Leaving

Note that these states are not a reflection of the current status of an M3DB node, but an indication of whether a given node has ever successfully bootstrapped and taken ownership of a given shard. For example, in a new cluster all the nodes will begin with all of their shards in the Initializing state. Once all the nodes finish bootstrapping, they will mark all of their shards as Available. If all the M3DB nodes are stopped at the same time, the cluster topology will still show all of the shards for all of the hosts as Available.

## Initializing State

The initializing state is the state in which all new host/shard combinations begin. For example, upon creating a new topology all the host/shard pairs will begin in the "Initializing" state and only once they have successfully bootstrapped will they transition to the "Available" state.

The Initializing state is not limited to new topology, however, as it can also occur during topology changes. For example, during a node add/replace the new host will begin with all of its shards in the Initializing state until it can stream the data it is missing from its peers. During a node removal, all of the hosts who receive new shards (as a result of taking over the responsibilities of the node that is leaving) will begin with those shards marked as Initializing until they can stream in the data from the node leaving the cluster, or one of its peers.

## Available State

Once a node with a shard in the Initializing state successfully bootstraps all of the data for that shard, it will mark that shard as Available (for the single host) in the cluster topology.

## Leaving State

The leaving state indicates that a node is attempting to leave the cluster. The purpose of this state is to allow the node to remain in the cluster long enough for the nodes that are taking over its responsibilities to stream data from it.


## Sample Cluster State Transitions - Node Add

Replication factor: 3

### Initial Topology

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   A   |
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   2   |   A   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  3   |   3   |   A   |

### Begin Node Add

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   L   |
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   2   |   L   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  3   |   3   |   L   |
|  4   |   1   |   I   |
|  4   |   2   |   I   |
|  4   |   3   |   I   |

### Complete Node Add

| Node | Shard | State |
|------|-------|-------|
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  4   |   1   |   A   |
|  4   |   2   |   A   |
|  4   |   3   |   A   |

## Sample Cluster State Transitions - Node Remove

Replication factor: 3

### Initial Topology

| Node | Shard | State |
|------|-------|-------|
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  4   |   1   |   L   |
|  4   |   2   |   L   |
|  4   |   3   |   L   |

### Begin Node Remove

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   I   |
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   2   |   I   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  3   |   3   |   I   |
|  4   |   1   |   L   |
|  4   |   2   |   L   |
|  4   |   3   |   L   |

### Complete Node Remove

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   A   |
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   2   |   A   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  3   |   3   |   A   |

## Sample Cluster State Transitions - Node Replace

Replication factor: 3

### Initial Topology

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   A   |
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   2   |   A   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  3   |   3   |   A   |

### Begin Node Replace

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   A   |
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   2   |   A   |
|  2   |   3   |   A   |
|  3   |   1   |   L   |
|  3   |   2   |   L   |
|  3   |   3   |   L   |
|  4   |   1   |   I   |
|  4   |   2   |   I   |
|  4   |   3   |   I   |

### Complete Node Replace

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   A   |
|  1   |   2   |   A   |
|  1   |   3   |   A   |
|  2   |   1   |   A   |
|  2   |   2   |   A   |
|  2   |   3   |   A   |
|  4   |   1   |   A   |
|  4   |   2   |   A   |
|  4   |   3   |   A   |