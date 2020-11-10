---
title: "Sharding"
weight: 2
---

Timeseries keys are hashed to a fixed set of virtual shards. Virtual shards are then assigned to physical nodes. M3DB can be configured to use any hashing function and a configured number of shards. By default [murmur3](https://en.wikipedia.org/wiki/MurmurHash) is used as the hashing function and 4096 virtual shards are configured.

## Benefits

Shards provide a variety of benefits throughout the M3DB stack:

1.  They make horizontal scaling easier and adding / removing nodes without downtime trivial at the cluster level.
2.  They provide more fine grained lock granularity at the memory level.
3.  They inform the filesystem organization in that data belonging to the same shard will be used / dropped together and can be kept in the same file.

## Replication

Logical shards are placed per virtual shard per replica with configurable isolation (zone aware, rack aware, etc). For instance, when using rack aware isolation, the set of datacenter racks that locate a replica’s data is distinct to the racks that locate all other replicas’ data.

Replication is synchronization during a write and depending on the consistency level configured will notify the client on whether a write succeeded or failed with respect to the consistency level and replication achieved.

## Replica

Each replica has its own assignment of a single logical shard per virtual shard.

Conceptually it can be defined as:

```golang
Replica {
  id uint32
  shards []Shard
}
```

## Shard state

Each shard can be conceptually defined as:

```golang
Shard {
  id uint32
  assignments []ShardAssignment
}

ShardAssignment {
  host Host
  state ShardState
}

enum ShardState {
  INITIALIZING,
  AVAILABLE,
  LEAVING
}
```

## Shard assignment

The assignment of shards is stored in etcd. When adding, removing or replacing a node shard goal states are assigned for each shard assigned.

For a write to appear as successful for a given replica it must succeed against all assigned hosts for that shard.  That means if there is a given shard with a host assigned as _LEAVING_ and another host assigned as _INITIALIZING_ for a given replica writes to both these hosts must appear as successful to return success for a write to that given replica.  Currently however only _AVAILABLE_ shards count towards consistency, the work to group the _LEAVING_ and _INITIALIZING_ shards together when calculating a write success/error is not complete, see [issue 417](https://github.com/m3db/m3/issues/417).

It is up to the nodes themselves to bootstrap shards when the assignment of new shards to it are discovered in the _INITIALIZING_ state and to transition the state to _AVAILABLE_ once bootstrapped by calling the cluster management APIs when done.  Using a compare and set this atomically removes the _LEAVING_ shard still assigned to the node that previously owned it and transitions the shard state on the new node from _INITIALIZING_ state to _AVAILABLE_.

Nodes will not start serving reads for the new shard until it is _AVAILABLE_, meaning not until they have bootstrapped data for those shards.

## Cluster operations

### Node add

When a node is added to the cluster it is assigned shards that relieves load fairly from the existing nodes.  The shards assigned to the new node will become _INITIALIZING_, the nodes then discover they need to be bootstrapped and will begin bootstrapping the data using all replicas available.  The shards that will be removed from the existing nodes are marked as _LEAVING_.

### Node down

A node needs to be explicitly taken out of the cluster.  If a node goes down and is unavailable the clients performing reads will be served an error from the replica for the shard range that the node owns.  During this time it will rely on reads from other replicas to continue uninterrupted operation.

### Node remove

When a node is removed the shards it owns are assigned to existing nodes in the cluster.  Remaining servers discover they are now in possession of shards that are _INITIALIZING_ and need to be bootstrapped and will begin bootstrapping the data using all replicas available.
