## M3DB Sharding and Replication

### Sharding

Data will be consistently hashed to a set of virtual nodes.  M3DB can be configured to use any hashing function and a configured number of shards.  For the sake of this documentation we’ll choose the following configuration:

**Hash:** Murmur32

**Shards:** 4096

e.g. for a four node single replica set:

Node 1: 0..1023

Node 2: 1023..2047

Node 3: 2048..3071

Node 4: 3072..4095

### Replication

Full sets of data, that is data from all 0 - 4096 shards will be replicated on separate nodes that are rack aware.  That is, the set of datacenter racks that locate a replica’s data should be distinct to the racks that locate all other replica’s data.

### Replica

Each replica will have its own view of each shard:

```
Replica struct {
  index uint32
  shards []Shard
}
```

### Shard state

Each shard will have the following data:

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
  AVAILABLE
}
```

### Shard assignment

The assignment of shards is performed by an independent sequentially consistent management service.  The management service simply assigns goal state for the shard assignment. 

For a write to appear as successful for a given replica it must succeed against all assigned hosts for that shard.  That means if there is a given shard with a host assigned as *AVAILABLE* and another host assigned as *INITIALIZING* for a given replica writes to both these hosts must appear as successful to appear as successful to that given replica. 

It is up to the nodes themselves to bootstrap shards when the assignment of new shards to it are discovered in the *INITIALIZING* state and to transition the state to *AVAILABLE* once bootstrapped by calling the management service back when done. 

The management server can then make a decision to remove the shard assignment from a host the shard was transferring from once it sees all shard assignments are *AVAILABLE* for a given shard.

Nodes will not start serving reads for the new shard sets they are assigned until they have bootstrapped data for those shards.

#### Node add

A node is added to the cluster by calling the management service.  The shards assigned to the new node will become *INITIALIZING* and the node will discover they need to be bootstrapped  and will begin bootstrapping the data using all replicas available to it.

#### Node down

A node will not be taken out of the cluster unless specifically removed by calling the management service.  If a node goes down and is unavailable the clients performing reads will be served an error from the replica for the shard range that the node owns.  During this time it will rely on reads from other replicas to continue uninterrupted operation.

#### Node remove

A node is removed by calling the management service.  The management service will shuffle shards amongst the replica set.  Remaining servers will discover they are now in possession of shards that need to be bootstrapped and will begin bootstrapping the data using all replicas available to it.

