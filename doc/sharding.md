## M3DB Sharding and Replication

### Sharding

Data is consistently hashed to a set of virtual nodes.  M3DB can be configured to use any hashing function and any number of shards.  For the sake of this documentation we’ll choose the following configuration:

**Hash:** Murmur32

**Shards:** 4096

e.g. for a four node, single replica set:

Node 1: 0..1023

Node 2: 1023..2047

Node 3: 2048..3071

Node 4: 3072..4095

### Replication

Full sets of data, i.e. data from all shards, will be replicated on separate, rack-aware nodes. That is, at most one replica of each shard is present on a single rack.

### Replica

Each replica has its own view of each shard:

```
Replica struct {
  index uint32
  shards []Shard
}
```

### Shard state

Each shard has the following data:

```
Shard struct {
  index uint32
  assignments []ShardAssignment
}

ShardAssignment struct {
  host Host
  state ShardState 
}

enum ShardState {
  INITIALIZING,
  LEAVING,
  AVAILABLE
}
```

### Shard assignment

Shard assignment is performed by an independent, sequentially-consistent management service.  The management service assigns goal states for shard assignment. 

For a write to appear as successful for a given replica it must succeed against enough assigned hosts for that shard, as defined by the system's write consistency level. The levels are:

* LevelOne: A write is successful if written to a single node.
* LevelMajority: A write is successful if written to a majority, i.e. a quorum, of nodes.
* LevelAll: A write is successful if written to all nodes.

Only writes to *AVAILABLE* nodes count as a successful write.

It is up to the nodes to bootstrap shards when the assignment of new shards to it are discovered in the *INITIALIZING* state and to transition the state to *AVAILABLE* once bootstrapped by calling the management service back when done. 

The management server can then make a decision to remove the shard assignment from a host the shard was transferring from once it sees all shard assignments are *AVAILABLE* for a given shard.

Nodes will not start serving reads for new shard sets they are assigned until they have bootstrapped data for those shards.

#### Node add

A node is added to the cluster by calling the management service.  The shards assigned to the new node will enter the *INITIALIZING* state, the node will discover they need to be bootstrapped and will begin bootstrapping the data using all available replicas.

#### Node down

A node will not be taken out of the cluster unless removed by calling the management service.  If a node goes down and is unavailable, the clients performing reads will be served an error from the replica for the shard range that the node owns.  During this time it will rely on reads from other replicas to continue uninterrupted operation.

#### Node remove

A node is removed by calling the management service.  The management service will shuffle shards amongst the replica set.  Remaining servers will discover they are now in possession of shards that need to be bootstrapped and will begin bootstrapping the data using all available replicas.

