## Sharding

Timeseries keys are hashed to a fixed set of virtual shards. Virtual shards are then assigned to physical nodes. M3DB can be configured to use any hashing function and a configured number of shards. By default [murmur3](https://en.wikipedia.org/wiki/MurmurHash) is used as the hashing function and 4096 virtual shards are configured.

## Replication

Logical shards are placed on per virtual shard per replica with configurable isolation (zone aware, rack aware, etc). For instance, when using rack aware isolation, the set of datacenter racks that locate a replica’s data are distinct to the racks that locate all other replica’s data.

Replication is synchronization during a write and depending on the consistency level configured will notify the client on whether a write succeeded or failed with respect to the consistency level and replication achieved.

## Replica

Each replica has its own assignment of a single logical shard per virtual shard.

Conceptually it can be defined as:

```
Replica {
  id uint32
  shards []Shard
}
```

For a

## Shard state

Each shard can be conceptually defined as:

```
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

The assignment of shards is performed by an independent sequentially consistent management service.  The management service simply assigns goal state for the shard assignment.

For a write to appear as successful for a given replica it must succeed against all assigned hosts for that shard.  That means if there is a given shard with a host assigned as *AVAILABLE* and another host assigned as *INITIALIZING* for a given replica writes to both these hosts must appear as successful to appear as successful to that given replica.

It is up to the nodes themselves to bootstrap shards when the assignment of new shards to it are discovered in the *INITIALIZING* state and to transition the state to *AVAILABLE* once bootstrapped by calling the management service back when done.

The management server can then make a decision to remove the shard assignment from a host the shard was transferring from once it sees all shard assignments are *AVAILABLE* for a given shard.

Nodes will not start serving reads for the new shard sets they are assigned until they have bootstrapped data for those shards.


## Cluster operations

### Node add

A node is added to the cluster by calling the management service.  The shards assigned to the new node will become *INITIALIZING* and the node will discover they need to be bootstrapped  and will begin bootstrapping the data using all replicas available to it.

### Node down

A node will not be taken out of the cluster unless specifically removed by calling the management service.  If a node goes down and is unavailable the clients performing reads will be served an error from the replica for the shard range that the node owns.  During this time it will rely on reads from other replicas to continue uninterrupted operation.

### Node remove

A node is removed by calling the management service.  The management service will shuffle shards amongst the replica set.  Remaining servers will discover they are now in possession of shards that need to be bootstrapped and will begin bootstrapping the data using all replicas available to it.
