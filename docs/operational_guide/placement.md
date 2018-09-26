# Placement

## Overview

**Note**: The words *placement* and *topology* are used interchangeably throughout the M3DB documentation and codebase.

A M3DB cluster has exactly one Placement. That placement maps the cluster's shard replicas to hosts. A cluster also has 0 or more namespaces, and each host serves every namespace for the shards it owns. In other words, if the cluster topology states that host A owns shards 1, 2, and 3 then host A will own shards 1, 2, 3 for all configured namespaces in the cluster.

M3DB stores its placement (mapping of which hosts are responsible for which shards) in [etcd](https://coreos.com/etcd/). There are three possible states that each host/shard pair can be in:

1. `Initializing`
2. `Available`
3. `Leaving`

Note that these states are not a reflection of the current status of an M3DB node, but an indication of whether a given node has ever successfully bootstrapped and taken ownership of a given shard (achieved goal state). For example, in a new cluster all the nodes will begin with all of their shards in the `Initializing` state. Once all the nodes finish bootstrapping, they will mark all of their shards as `Available`. If all the M3DB nodes are stopped at the same time, the cluster placement will still show all of the shards for all of the hosts as `Available`.

## Initializing State

The `Initializing` state is the state in which all new host/shard combinations begin. For example, upon creating a new placement all the host/shard pairs will begin in the `Initializing` state and only once they have successfully bootstrapped will they transition to the `Available`` state.

The `Initializing` state is not limited to new placement, however, as it can also occur during placement changes. For example, during a node add/replace the new host will begin with all of its shards in the `Initializing` state until it can stream the data it is missing from its peers. During a node removal, all of the hosts who receive new shards (as a result of taking over the responsibilities of the node that is leaving) will begin with those shards marked as `Initializing` until they can stream in the data from the node leaving the cluster, or one of its peers.

## Available State

Once a node with a shard in the `Initializing` state successfully bootstraps all of the data for that shard, it will mark that shard as `Available` (for the single host) in the cluster placement.

## Leaving State

The `Leaving` state indicates that a node has been marked for removal from the cluster. The purpose of this state is to allow the node to remain in the cluster long enough for the nodes that are taking over its responsibilities to stream data from it.

## Sample Cluster State Transitions - Node Add

Replication factor: 3

                                 ┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐       ┌─────────────────┐
                                 │     Host 1      │          │     Host 2      │        │     Host 3      │       │     Host 4      │
┌──────────────────────────┬─────┴─────────────────┴─────┬────┴─────────────────┴────┬───┴─────────────────┴───┬───┴─────────────────┴───┐
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │   Shard 1: Available    │ │ │  Shard 1: Available   │ │ │  Shard 1: Available  ││                         │
│     Initial Placement    │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ │ │  Shard 2: Available  ││                         │
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
│      Begin Node Add      │ │    Shard 2: Available   │ │ │   Shard 2: Leaving    │ │ │  Shard 2: Available  │││Shard 2: Initializing │ │
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
│     Complete Node Add    │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │ │  Shard 2: Available  │││  Shard 2: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││  Shard 3: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│└──────────────────────┘ │
│                          │                             │                           │                         │                         │
└──────────────────────────┴─────────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘


## Sample Cluster State Transitions - Node Remove

Replication factor: 3

                                 ┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐       ┌─────────────────┐
                                 │     Host 1      │          │     Host 2      │        │     Host 3      │       │     Host 4      │
┌──────────────────────────┬─────┴─────────────────┴─────┬────┴─────────────────┴────┬───┴─────────────────┴───┬───┴─────────────────┴───┐
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │   Shard 2: Available    │ │ │  Shard 1: Available   │ │ │  Shard 1: Available  │││  Shard 1: Available  │ │
│     Initial Placement    │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │ │  Shard 2: Available  │││  Shard 2: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││  Shard 3: Available  │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ │                         │ │ │                       │ │ │                      │││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│└──────────────────────┘ │
├──────────────────────────┼─────────────────────────────┼───────────────────────────┼─────────────────────────┼─────────────────────────┤
│                          │                             │                           │                         │                         │
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │┌───────────────────────┐│┌──────────────────────┐ │
│                          │ │                         │ │ │                       │ ││                       │││                      │ │
│                          │ │                         │ │ │                       │ ││  Shard 1: Available   │││                      │ │
│                          │ │   Shard 1: Initializing │ │ │  Shard 1: Available   │ ││  Shard 2: Available   │││   Shard 1: Leaving   │ │
│     Begin Node Remove    │ │   Shard 2: Available    │ │ │  Shard 2: Initializing│ ││  Shard 3: Initializing│││   Shard 2: Leaving   │ │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ ││                       │││   Shard 3: Leaving   │ │
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
│     Complete Node Add    │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ │ │  Shard 2: Available  ││                         │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │ │  Shard 3: Available  ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │ └──────────────────────┘│                         │
│                          │                             │                           │                         │                         │
└──────────────────────────┴─────────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘

## Sample Cluster State Transitions - Node Replace

Replication factor: 3

                                 ┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐       ┌─────────────────┐
                                 │     Host 1      │          │     Host 2      │        │     Host 3      │       │     Host 4      │
┌──────────────────────────┬─────┴─────────────────┴─────┬────┴─────────────────┴────┬───┴─────────────────┴───┬───┴─────────────────┴───┐
│                          │ ┌─────────────────────────┐ │ ┌───────────────────────┐ │ ┌──────────────────────┐│                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │                         │ │ │                       │ │ │                      ││                         │
│                          │ │   Shard 1: Available    │ │ │  Shard 1: Available   │ │ │  Shard 1: Available  ││                         │
│     Initial Placement    │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ │ │  Shard 2: Available  ││                         │
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
│     Begin Node Remove    │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ ││   Shard 2: Leaving    │││Shard 2: Initializing │ │
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
│                          │ │    Shard 1: Avaiable    │ │ │  Shard 1: Available   │ │                         ││  Shard 1: Available  │ │
│     Complete Node Add    │ │   Shard 2: Available    │ │ │  Shard 2: Available   │ │                         ││  Shard 2: Available  │ │
│                          │ │   Shard 3: Available    │ │ │  Shard 3: Available   │ │                         ││  Shard 3: Available  │ │
│                          │ │                         │ │ │                       │ │                         ││                      │ │
│                          │ │                         │ │ │                       │ │                         ││                      │ │
│                          │ └─────────────────────────┘ │ └───────────────────────┘ │                         │└──────────────────────┘ │
│                          │                             │                           │                         │                         │
└──────────────────────────┴─────────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘