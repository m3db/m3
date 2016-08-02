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
  minVersion uint64 
}

ShardAssignment struct {
  host Host
  state ShardState 
}

enum ShardState {
  PROPOSED,
  INITIALIZING, 
  AVAILABLE
}
```

### Shard assignment

The assignment of shards is performed by an independent sequentially consistent management service.  The management service simply assigns goal state for the shard assignment. 

For a write to appear as successful for a given replica it must succeed against all assigned hosts for that shard.  That means if there is a given shard with a host assigned as *AVAILABLE* and another host assigned as *INITIALIZING* for a given replica writes to both these hosts must appear as successful to appear as successful to that given replica. 

When a change is made the node receiving ownership of a shard is assigned to the shard in the *PROPOSED* state.  At this point in time it will begin taking writes.  This ensures that once all nodes see this state, switching to the *INITIALIZING* state is safe as all nodes already treat writes failing to this node as a failure to write to the given replica.

Currently transitioning from the *PROPOSED* to *INITIALIZING* state is a manual step with verification from instrumentation that all participating members see the proposed, however this can too be automated.

It is up to the nodes themselves to bootstrap and when the assignment changes to bootstrap new shards that were assigned to it in the *INITIALIZING* state and to transition the state to *AVAILABLE* once bootstrapped by calling the management service back when done. 

The management server can then make a decision to remove the shard assignment from a host the shard was transferring from once it sees all shard assignments are *AVAILABLE* for a given shard.

Nodes will not start serving reads for the new shard sets they are assigned but don’t have until they have bootstrapped data for that shard set.

#### Node add

A node is added to the cluster by calling the management service.  The management service will specify the node as *PROPOSED* for the shards the new node will own and writes will begin being sent to the new node.  The node will discover it is now in possession of a set of shards that are proposed and will begin accepting writes.  Once the proposal passes the shards will become *INITIALIZING* and the node will discover they need to be bootstrapped  and will begin bootstrapping the data using all replicas available to it.

#### Node down

A node will not be taken out of the cluster unless specifically removed by calling the management service.  If a node goes down and is unavailable the clients performing reads will be served an error from the replica for the shard range that the node owns.  During this time it will rely on reads from other replicas to continue uninterrupted operation.

#### Node remove

A node is removed by calling the management service.  The management service will shuffle 1/n shards amongst the replica set by calculating the partition set divided by the remaining active servers for that replica.  Remaining servers will discover they are now in possession of shards that need to be bootstrapped and will begin bootstrapping the data using all replicas available to it.

Nodes will not start serving reads for the new shard sets they are assigned but don’t have until they have bootstrapped data for that shard set.

### Shard assignment consistency

To ensure a consistent view of the shard assignment each node will require an operation to be received with a configuration ID the client saw.  The management service keeps the configuration ID alongside the latest configuration and each change to the configuration will increment the configuration ID.  

Each shard in the configuration will have a field that declares which minimum version it is compatible with.  Every time a shard is modified in a replica as part of a configuration change the minimum version is set to the new configuration ID in the minVersion field. 

This allows clients that are out of date to gracefully move across configurations in a way that let’s them be slightly inconsistent and will guarantee their consistency level is met.

