# Bootstrapping

## Introduction

We recommend reading the [topology operational guide](topology.md) before reading the rest of this document.

When an M3DB node is turned on (or experiences a topology change) it needs to go through a bootstrapping process to determine the integrity of data that it has, replay writes from the commit log, and/or stream missing data from its peers. In most cases, as long as you're running with the default and recommended bootstrapper configuration of: `filesystem,commitlog,peers,uninitialized_topology` then you should not need to worry about the bootstrapping process at all and M3DB will take care of doing the right thing such that you don't lose data and consistency guarantees are met.

Generally speaking, we recommend that operators do not modify the bootstrappers configuration, but in the rare case that you need to this document is designed to help you understand the implications of doing so.

M3DB currently supports 5 different bootstrappers:

1. filesystem
2. commitlog
3. peers
4. uninitialized_topology
5. noop_all

When the bootstrapping process begins, M3DB nodes need to determine two things:

1. What shards they should bootstrap, which can be determined from the cluster topology.
2. What time-ranges they need to bootstrap those shards for, which can be determined from the namespace retention.

For example, imagine an M3DB node that is responsible for shards 1, 5, 13, and 25 according to the cluster topology. In addition, it has a single namespace called "metrics" with a retention of 48 hours. When the M3DB node is started, the node will determine that it needs to bootstrap shards 1, 5, 13, and 25 for the time range starting at the current time and ending 48 hours ago. In order to obtain all this data, it will run the configured bootstrappers in the specified order. Every bootstrapper will notify the bootstrapping process of which shard/ranges it was able to bootstrap and the bootstrapping process will continue working its way through the list of bootstrappers until all the shards/ranges it requires have been marked as fulfilled. Otherwise the M3DB node will fail to start.

### Filesystem Bootstrapper

The filesystem bootstrapper's responsibility is to determine which immutable [fileset files](../m3db/architecture/storage.md) exist on disk, and if so, mark them as fulfilled. The filesystem bootstrapper achieves this by scanning M3DB's directory structure and determining which fileset files already exist on disk. Unlike the other bootstrappers, the filesystem bootstrapper does not need to load any data into memory, it simply verifies the checksums of the data on disk and the M3DB node itself will handle reading (and caching) the data dynamically once it begins to serve reads.

### Commitlog Bootstrapper

The commitlog bootstrapper's responsibility is to read the commitlog and snapshot (compacted commitlogs) files on disk and recover any data that has not yet been written out as an immutable fileset file. Unlike the filesystem bootstrapper, the commit log bootstrapper cannot simply check which files are on disk in order to determine if it can satisfy a bootstrap request. Instead, the commitlog bootstrapper determines whether it can satisfy a bootstrap request using a simple heuristic.

On a shard-by-shard basis, the commitlog bootstrapper will consult the cluster topology to see if the host it is running on has ever achieved the "Available" status for the specified shard. If so, then the commit log bootstrapper should have all the data since the last fileset file was flushed and will return that it can satisfy any time range for that shard. In other words, the commit log bootstrapper is all-or-nothing for a given shard: it will either return that it can satisfy any time range for a given shard or none at all. In addition, the commitlog bootstrapper *assumes* it is running after the filesystem bootstrapper. M3DB will not allow you to run with a configuration where the filesystem bootstrappe is placed after the commitlog bootstrapper, but it will allow you to run the commitlog bootstrapper without the filesystem bootstrapper which can result in loss of data, depending on the workload.

### Peers Bootstrapper

The peer bootstrapper's responsibility is to stream in data for shard/ranges from other M3DB nodes (peers) in the cluster. This bootstrapper is only useful in M3DB clusters with more than a single node *and* where the replication factor is set to a value larger than 1. The peers bootstrapper will determine whether or not it can satisfy a bootstrap request on a shard-by-shard basis by consulting the cluster topology and determining if there are enough peers to satisfy the bootstrap request. For example, imagine the following M3DB topology where node 1 is trying to perform a peer bootstrap:

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   I   |
|  1   |   2   |   I   |
|  1   |   3   |   I   |
|  2   |   1   |   I   |
|  2   |   2   |   I   |
|  2   |   3   |   I   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  3   |   3   |   A   |

In this case, the peer bootstrapper running on node 1 will not be able to fullfill any requests because node 2 is in the Initializing state for all of its shards and cannot fulfill bootstrap requests. This means that node 1's peer bootstrapper cannot meet its default consistency level of majority for bootstrapping (1 < 2 which is majority with a replication factor of 3). On the other hand, node 1 would be able to peer bootstrap in the following topology because its peers (nodes 2/3) are available for all of their shards:

| Node | Shard | State |
|------|-------|-------|
|  1   |   1   |   I   |
|  1   |   2   |   I   |
|  1   |   3   |   I   |
|  2   |   1   |   A   |
|  2   |   2   |   A   |
|  2   |   3   |   A   |
|  3   |   1   |   A   |
|  3   |   2   |   A   |
|  3   |   3   |   A   |

Note that a bootstrap consistency level of majority is the default value, but can be modified by changing the value of the key "m3db.client.bootstrap-consistency-level" in EtcD to one of: "none", "one", "unstrict_majority" (attempt to read from majority, but settle for less if any errors occur), "majority" (strict majority), and "all".

**Note**: Any bootstrappers configuration that does not include the peers bootstrapper will be unable to handle dynamic topology changes of any kind.

### Uninitialized Topology Bootstrapper

The purpose of the uninitialzied topology bootstrapper is to succeed bootstraps for all time ranges for shards that have never been completely bootstrapped (at a cluster level). This allows us to run the default bootstrapper configuration of: `filesystem,commitlog,peers,topology_uninitialized` such that the filesystem and commitlog bootstrappers are used by default in node restarts, the peer bootstrapper is used for node adds/removes/replaces, and bootstraps still succeed for brand new topologies where both the commitlog and peers bootstrappers will be unable to succeed any bootstraps. In other words, the uninitialized topology bootstrapper allows us to place the commitlog bootstrapper *before* the peers bootstrapper and still succeed bootstraps with brand new topologies without resorting to using the noop-all bootstrapper which suceeds bootstraps for all shard/time-ranges regardless of the status of the topology.

The uninitialized topology bootstrapper determines whether a topology is "new" for a given shard by counting the number of hosts in the Initializing state and Leaving states and if the number of Initializing - Leaving > 0 than it succeeds the bootstrap because that means the topology has never reached a state where all hosts are Available.

### No Operational All Bootstrapper

The noop_all bootstrapper succeeds all bootstraps regardless of requests shards/time ranges.

### Bootstrappers Configuration

Now that we've gone over the various bootstrappers, lets consider how M3DB will behave in different configurations. Note that we include uninitialized_topology at the end of all the lists of bootstrappers because its required to get a new topology up and running in the first place, but is not required after that (although leaving it in has no detrimental effects). Also note that any configuration that does not include the peers bootstrapper will not be able to handle dynamic topology changes like node adds/removes/replaces.

#### filesystem,commitlog,peers,uninitialized_topology (default)

This is the default bootstrappers configuration for M3DB and will behave "as expected" in the sense that it will maintain M3DB's consistency guarantees at all times, handle node adds/replaces/removes correctly, and still work with brand new topologies.

In the general case, the node will use only the filesystem and commitlog bootstrappers on node startup. However, in the case of a node add/remove/replace, the commitlog bootstrapper will detect that it is unable to fulfill the bootstrap request (because the node has never reached the Available state) and defer to the peers bootstrapper to stream in the data.

Additionally, if it is a brand new topology where even the peers bootstrapper cannot fulfill the bootstrap, this will be detected by the uninitialized_topology bootstrapper which will succeed the bootstrap.

#### filesystem,commitlog,uninitialized_topology

This bootstrapping configuration will work just fine if nodes are never added/replaced/removed, but will fail when attempting a node add/replace/remove.

#### peers,uninitialized_topology

Everytime a node is restarted, it will attempt to stream in *all* of the data that it is responsible for from its peers, completely ignoring the immutable fileset files it already has on disk. We do not recommend running in this mode as it can lead to violations of M3DB's consistency guarantees due to the fact that the commit logs are being ignored, however, it *can* be useful if you want to repair the data on a node by forcing it to stream from its peers.

#### filesystem,uninitialized_topology

Everytime a node is restarted it will utilize the immutable fileset files its already written out to disk, but any data that it had received since it wrote out the last set of immutable files will be lost.

#### commitlog,uninitialized_topology

Everytime a node is restarted it will read all the commit log and snapshot files it has on disk, but it will ignore all the data in the immutable fileset files that it has already written.
