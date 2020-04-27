---
title: "Topology and placement"
date: 2020-04-21T21:01:48-04:00
draft: true
---

Placement
Overview
Note: The words placement and topology are used interchangeably throughout the M3DB documentation and codebase.
A M3DB cluster has exactly one Placement. That placement maps the cluster's shard replicas to nodes. A cluster also has 0 or more namespaces (analogous to tables in other databases), and each node serves every namespace for the shards it owns. In other words, if the cluster topology states that node A owns shards 1, 2, and 3 then node A will own shards 1, 2, 3 for all configured namespaces in the cluster.
M3DB stores its placement (mapping of which NODES are responsible for which shards) in etcd. There are three possible states that each node/shard pair can be in:
Initializing
Available
Leaving
Note that these states are not a reflection of the current status of an M3DB node, but an indication of whether a given node has ever successfully bootstrapped and taken ownership of a given shard (achieved goal state). For example, in a new cluster all the nodes will begin with all of their shards in the Initializing state. Once all the nodes finish bootstrapping, they will mark all of their shards as Available. If all the M3DB nodes are stopped at the same time, the cluster placement will still show all of the shards for all of the nodes as Available.
Initializing State
The Initializing state is the state in which all new node/shard combinations begin. For example, upon creating a new placement all the node/shard pairs will begin in the Initializing state and only once they have successfully bootstrapped will they transition to the Available state.
The Initializing state is not limited to new placement, however, as it can also occur during placement changes. For example, during a node add/replace the new node will begin with all of its shards in the Initializing state until it can stream the data it is missing from its peers. During a node removal, all of the nodes who receive new shards (as a result of taking over the responsibilities of the node that is leaving) will begin with those shards marked as Initializing until they can stream in the data from the node leaving the cluster, or one of its peers.
Available State
Once a node with a shard in the Initializing state successfully bootstraps all of the data for that shard, it will mark that shard as Available (for the single node) in the cluster placement.
Leaving State
The Leaving state indicates that a node has been marked for removal from the cluster. The purpose of this state is to allow the node to remain in the cluster long enough for the nodes that are taking over its responsibilities to stream data from it.
Sample Cluster State Transitions - Node Add
Node adds are performed by adding the new node to the placement. Some portion of the existing shards will be assigned to the new node based on its weight, and they will begin in the Initializing state. Similarly, the shards will be marked as Leaving on the node that are destined to lose ownership of them. Once the new node finishes bootstrapping the shards, it will update the placement to indicate that the shards it owns are Available and that the Leaving node should no longer own that shard in the placement.
Replication factor: 3

                                 ┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐       ┌─────────────────┐
                                 │     Node A      │          │     Node B      │        │     Node C      │       │     Node D      │
┌──────────────────────────┬─────┴─────────────────┴─────┬────┴─────────────────┴────┬───┴─────────────────┴───┬───┴─────────────────┴───┐
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │   Shard 1: Available    │ │ │  Shard 1: Available   │ │ │  Shard 1: Available  ││                         │
│  1) Initial Placement    │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ │ │  Shard 2: Available  ││                         │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │ │  Shard 3: Available  ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│                         │
├──────────────────────────┼─────────────────────────────┼───────────────────────────┼─────────────────────────┼─────────────────────────┤
│                          │                             │                           │                         │                         │
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │    Shard 1: Leaving     │ │ │   Shard 1: Available  │ │ │  Shard 1: Available  │││Shard 1: Initializing │ │
│   2) Begin Node Add      │ │    Shard 2: Available   │ │ │   Shard 2: Leaving    │ │ │  Shard 2: Available  │││Shard 2: Initializing │ │
│                          │ │    Shard 3: Available   │ │ │   Shard 3: Available  │ │ │  Shard 3: Leaving    │││Shard 3: Initializing │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│└──────────────────────┘ │
│                          │                             │                           │                         │                         │
├──────────────────────────┼─────────────────────────────┼───────────────────────────┼─────────────────────────┼─────────────────────────┤
│                          │                             │                           │                         │                         │
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │   Shard 2: Available    │ │ │  Shard 1: Available   │ │ │  Shard 1: Available  │││  Shard 1: Available  │ │
│  3) Complete Node Add    │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │ │  Shard 2: Available  │││  Shard 2: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││  Shard 3: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│└──────────────────────┘ │
│                          │                             │                           │                         │                         │
└──────────────────────────┴─────────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘

Overview
M3DB was designed from the ground up to be a distributed (clustered) database that is availability zone or rack aware (by using isolation groups). Clusters will seamlessly scale with your data, and you can start with a small number of nodes and grow it to a size of several hundred nodes with no downtime or expensive migrations.
Before reading the rest of this document, we recommend familiarizing yourself with the M3DB placement documentation
Note: The primary limiting factor for the maximum size of an M3DB cluster is the number of shards. Picking an appropriate number of shards is more of an art than a science, but our recommendation is as follows:
The number of shards that M3DB uses is configurable and there are a couple of key points to note when deciding the number to use. The more nodes you have, the more shards you want because you want the shards to be evenly distributed amongst your nodes. However, because each shard requires more files to be created, you also don’t want to have too many shards per node. This is due to the fact each bit of data needs to be repartitioned and moved around the cluster (i.e. every bit of data needs to be moved all at once). Below are some guidelines depending on how many nodes you will have in your cluster eventually - you will need to decide the number of shards up front, you cannot change this once the cluster is created.
Number of Nodes
Number of Shards
3
64
6
128
12
256
24
512
48
1024
128+
4096
