---
title: "Placement"
weight: 6
---

**Note**: The words _placement_ and _topology_ are used interchangeably throughout the M3DB documentation and codebase.

A M3DB cluster has exactly one Placement. That placement maps the cluster's shard replicas to nodes. A cluster also has 0 or more namespaces (analogous to tables in other databases), and each node serves every namespace for the shards it owns. In other words, if the cluster topology states that node A owns shards 1, 2, and 3 then node A will own shards 1, 2, 3 for all configured namespaces in the cluster.

M3DB stores its placement (mapping of which NODES are responsible for which shards) in [etcd](https://etcd.io/). There are three possible states that each node/shard pair can be in:

1.  `Initializing`
2.  `Available`
3.  `Leaving`

Note that these states are not a reflection of the current status of an M3DB node, but an indication of whether a given node has ever successfully bootstrapped and taken ownership of a given shard (achieved goal state). For example, in a new cluster all the nodes will begin with all of their shards in the `Initializing` state. Once all the nodes finish bootstrapping, they will mark all of their shards as `Available`. If all the M3DB nodes are stopped at the same time, the cluster placement will still show all of the shards for all of the nodes as `Available`.

## Initializing State

The `Initializing` state is the state in which all new node/shard combinations begin. For example, upon creating a new placement all the node/shard pairs will begin in the `Initializing` state and only once they have successfully bootstrapped will they transition to the `Available` state.

The `Initializing` state is not limited to new placement, however, as it can also occur during placement changes. For example, during a node add/replace the new node will begin with all of its shards in the `Initializing` state until it can stream the data it is missing from its peers. During a node removal, all of the nodes who receive new shards (as a result of taking over the responsibilities of the node that is leaving) will begin with those shards marked as `Initializing` until they can stream in the data from the node leaving the cluster, or one of its peers.

## Available State

Once a node with a shard in the `Initializing` state successfully bootstraps all of the data for that shard, it will mark that shard as `Available` (for the single node) in the cluster placement.

## Leaving State

The `Leaving` state indicates that a node has been marked for removal from the cluster. The purpose of this state is to allow the node to remain in the cluster long enough for the nodes that are taking over its responsibilities to stream data from it.

## Sample Cluster State Transitions - Node Add

Node adds are performed by adding the new node to the placement. Some portion of the existing shards will be assigned to the new node based on its weight, and they will begin in the `Initializing` state. Similarly, the shards will be marked as `Leaving` on the node that are destined to lose ownership of them. Once the new node finishes bootstrapping the shards, it will update the placement to indicate that the shards it owns are `Available` and that the `Leaving` node should no longer own that shard in the placement.

```yaml
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
```

## Sample Cluster State Transitions - Node Remove

Node removes are performed by updating the placement such that all the shards on the node that will be removed from the cluster are marked as `Leaving` and those shards are distributed to the remaining nodes (based on their weight) and assigned a state of `Initializing`. Once the existing nodes that are taking ownership of the leaving nodes shards finish bootstrapping, they will update the placement to indicate that the shards that they just acquired are `Available` and that the leaving node should no longer own those shards in the placement.

```yaml
Replication factor: 3

                                 ┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐       ┌─────────────────┐
                                 │     Node A      │          │     Node B      │        │     Node C      │       │     Node D      │
┌──────────────────────────┬─────┴─────────────────┴─────┬────┴─────────────────┴────┬───┴─────────────────┴───┬───┴─────────────────┴───┐
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │   Shard 2: Available    │ │ │  Shard 1: Available   │ │ │  Shard 1: Available  │││  Shard 1: Available  │ │
│  1) Initial Placement    │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │ │  Shard 2: Available  │││  Shard 2: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││  Shard 3: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│└──────────────────────┘ │
├──────────────────────────┼─────────────────────────────┼───────────────────────────┼─────────────────────────┼─────────────────────────┤
│                          │                             │                           │                         │                         │
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │┌───────────────────────┐│┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ │   Shard 1: Initializing │ │ │  Shard 1: Available   │ ││  Shard 1: Available   │││   Shard 1: Leaving   │ │
│  2) Begin Node Remove    │ │   Shard 2: Available    │ │ │  Shard 2: Initializing│ ││  Shard 2: Available   │││   Shard 2: Leaving   │ │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ ││  Shard 3: Initializing│││   Shard 3: Leaving   │ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │└───────────────────────┘│└──────────────────────┘ │
│                          │                             │                           │                         │                         │
├──────────────────────────┼─────────────────────────────┼───────────────────────────┼─────────────────────────┼─────────────────────────┤
│                          │                             │                           │                         │                         │
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │    Shard 1: Avaiable    │ │ │  Shard 1: Available   │ │ │  Shard 1: Available  ││                         │
│  3) Complete Node Remove │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ │ │  Shard 2: Available  ││                         │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │ │  Shard 3: Available  ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│                         │
│                          │                             │                           │                         │                         │
└──────────────────────────┴─────────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

## Sample Cluster State Transitions - Node Replace

Node replaces are performed by updating the placement such that all the shards on the node that will be removed from the cluster are marked as `Leaving` and those shards are all added to the node that is being added and assigned a state of `Initializing`. Once the replacement node finishes bootstrapping, it will update the placement to indicate that the shards that it acquired are `Available` and that the leaving node should no longer own those shards in the placement.

```yaml
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
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │┌───────────────────────┐│┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ │   Shard 1: Available    │ │ │  Shard 1: Available   │ ││   Shard 1: Leaving    │││Shard 1: Initializing │ │
│  2) Begin Node Replace   │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ ││   Shard 2: Leaving    │││Shard 2: Initializing │ │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ ││   Shard 3: Leaving    │││Shard 3: Initializing │ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │└───────────────────────┘│└──────────────────────┘ │
│                          │                             │                           │                         │                         │
├──────────────────────────┼─────────────────────────────┼───────────────────────────┼─────────────────────────┼─────────────────────────┤
│                          │                             │                           │                         │                         │
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │                         │┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ │                         ││                      │ │
│                          │ │                         │ │ │                       │ │                         ││                      │ │
│                          │ │   Shard 1: Avaiable     │ │ │  Shard 1: Available   │ │                         ││  Shard 1: Available  │ │
│  3) Complete Node Replace│ │   Shard 2: Available    │ │ │  Shard 2: Available   │ │                         ││  Shard 2: Available  │ │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │                         ││  Shard 3: Available  │ │
│                          │ │                         │ │ │                       │ │                         ││                      │ │
│                          │ │                         │ │ │                       │ │                         ││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │                         │└──────────────────────┘ │
│                          │                             │                           │                         │                         │
└──────────────────────────┴─────────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

## Cluster State Transitions - Placement Updates Initiation

The diagram below depicts the sequence of events that happen during a node replace and illustrates which entity is performing the placement update (in etcd) at each step.

```yaml
 ┌────────────────────────────────┐
 │             Node A             │
 │                                │
 │       Shard 1: Available       │
 │       Shard 2: Available       │     Operator performs node replace by
 │       Shard 3: Available       │      updating placement in etcd such
 │                                │     that shards on node A are marked
 └────────────────────────────────┤     Leaving and shards on node B are
                                  │            marked Initializing
                                  └─────────────────────────────────┐
                                                                    │
                                                                    │
                                                                    │
                                                                    │
                                                                    │
                                                                    ▼
                                                   ┌────────────────────────────────┐
                                                   │             Node A             │
                                                   │                                │
                                                   │        Shard 1: Leaving        │
                                                   │        Shard 2: Leaving        │
                                                   │        Shard 3: Leaving        │
                                                   │                                │
                                                   └────────────────────────────────┘

                                                   ┌────────────────────────────────┐
                                                   │             Node B             │
                                                   │                                │
                                                   │     Shard 1: Initializing      │
┌────────────────────────────────┐                 │     Shard 2: Initializing      │
│                                │                 │     Shard 3: Initializing      │
│                                │                 │                                │
│             Node A             │                 └────────────────────────────────┘
│                                │                                  │
│                                │                                  │
│                                │                                  │
└────────────────────────────────┘                                  │
                                                                    │
┌────────────────────────────────┐                                  │
│             Node B             │                                  │
│                                │                                  │
│       Shard 1: Available       │   Node B completes bootstrapping and
│       Shard 2: Available       │◀────updates placement (via etcd) to
│       Shard 3: Available       │    indicate shard state is Available and
│                                │    that Node A should no longer own any shards
└────────────────────────────────┘
```
