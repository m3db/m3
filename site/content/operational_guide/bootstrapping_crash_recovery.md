---
title: "Bootstrapping & Crash Recovery"
weight: 9
---
## Introduction

We recommend reading the [placement operational guide](/v1.0/docs/operational_guide/placement) before reading the rest of this document.

When an M3DB node is turned on (goes through a placement change) it needs to go through a bootstrapping process to determine the integrity of data that it has, replay writes from the commit log, and/or stream missing data from its peers. In most cases, as long as you're running with the default and recommended bootstrapper configuration of: `filesystem,commitlog,peers,uninitialized_topology` then you should not need to worry about the bootstrapping process at all and M3DB will take care of doing the right thing such that you don't lose data and consistency guarantees are met. Note that the order of the configured bootstrappers *does* matter.

Generally speaking, we recommend that operators do not modify the bootstrappers configuration, but in the rare case that you to, this document is designed to help you understand the implications of doing so.

M3DB currently supports 5 different bootstrappers:

1. `filesystem`
2. `commitlog`
3. `peers`
4. `uninitialized_topology`
5. `noop-all`

When the bootstrapping process begins, M3DB nodes need to determine two things:

1. What shards the bootstrapping node should bootstrap, which can be determined from the cluster placement.
2. What time-ranges the bootstrapping node needs to bootstrap those shards for, which can be determined from the namespace retention.

For example, imagine a M3DB node that is responsible for shards 1, 5, 13, and 25 according to the cluster placement. In addition, it has a single namespace called "metrics" with a retention of 48 hours. When the M3DB node is started, the node will determine that it needs to bootstrap shards 1, 5, 13, and 25 for the time range starting at the current time and ending 48 hours ago. In order to obtain all this data, it will run the configured bootstrappers in the specified order. Every bootstrapper will notify the bootstrapping process of which shard/ranges it was able to bootstrap and the bootstrapping process will continue working its way through the list of bootstrappers until all the shards/ranges required have been marked as fulfilled. Otherwise the M3DB node will fail to start.

## Bootstrappers

### Filesystem Bootstrapper

The `filesystem` bootstrapper's responsibility is to determine which immutable [Fileset files](/v1.0/docs/m3db/architecture/storage) exist on disk, and if so, mark them as fulfilled. The `filesystem` bootstrapper achieves this by scanning M3DB's directory structure and determining which Fileset files exist on disk. Unlike the other bootstrappers, the `filesystem` bootstrapper does not need to load any data into memory, it simply verifies the checksums of the data on disk and other components of the M3DB node will handle reading (and caching) the data dynamically once it begins to serve reads.

### Commitlog Bootstrapper

The `commitlog` bootstrapper's responsibility is to read the commitlog and snapshot (compacted commitlogs) files on disk and recover any data that has not yet been written out as an immutable Fileset file. Unlike the `filesystem` bootstrapper, the commit log bootstrapper cannot simply check which files are on disk in order to determine if it can satisfy a bootstrap request. Instead, the `commitlog` bootstrapper determines whether it can satisfy a bootstrap request using a simple heuristic.

On a shard-by-shard basis, the `commitlog` bootstrapper will consult the cluster placement to see if the node it is running on has ever achieved the `Available` status for the specified shard. If so, then the commit log bootstrapper should have all the data since the last Fileset file was flushed and will return that it can satisfy any time range for that shard. In other words, the commit log bootstrapper is all-or-nothing for a given shard: it will either return that it can satisfy any time range for a given shard or none at all. In addition, the `commitlog` bootstrapper *assumes* it is running after the `filesystem` bootstrapper. M3DB will not allow you to run with a configuration where the `filesystem` bootstrapper is placed after the `commitlog` bootstrapper, but it will allow you to run the `commitlog` bootstrapper without the `filesystem` bootstrapper which can result in loss of data, depending on the workload.

### Peers Bootstrapper

The `peers` bootstrapper's responsibility is to stream in data for shard/ranges from other M3DB nodes (peers) in the cluster. This bootstrapper is only useful in M3DB clusters with more than a single node *and* where the replication factor is set to a value larger than 1. The `peers` bootstrapper will determine whether or not it can satisfy a bootstrap request on a shard-by-shard basis by consulting the cluster placement and determining if there are enough peers to satisfy the bootstrap request. For example, imagine the following M3DB placement where node A is trying to perform a peer bootstrap:

```
    ┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐
    │     Node A      │          │     Node B      │        │     Node C      │
────┴─────────────────┴──────────┴─────────────────┴────────┴─────────────────┴───
┌─────────────────────────┐   ┌───────────────────────┐   ┌──────────────────────┐
│                         │   │                       │   │                      │
│                         │   │                       │   │                      │
│  Shard 1: Initializing  │   │ Shard 1: Initializing │   │  Shard 1: Available  │
│  Shard 2: Initializing  │   │ Shard 2: Initializing │   │  Shard 2: Available  │
│  Shard 3: Initializing  │   │ Shard 3: Initializing │   │  Shard 3: Available  │
│                         │   │                       │   │                      │
│                         │   │                       │   │                      │
└─────────────────────────┘   └───────────────────────┘   └──────────────────────┘
```

In this case, the `peers` bootstrapper running on node A will not be able to fullfill any requests because node B is in the `Initializing` state for all of its shards and cannot fulfill bootstrap requests. This means that node A's `peers` bootstrapper cannot meet its default consistency level of majority for bootstrapping (1 < 2 which is majority with a replication factor of 3). On the other hand, node A would be able to peer bootstrap its shards in the following placement because its peers (nodes B/C) have sufficient replicas of the shards it needs in the `Available` state:

```
    ┌─────────────────┐          ┌─────────────────┐        ┌─────────────────┐
    │     Node A      │          │     Node B      │        │     Node C      │
────┴─────────────────┴──────────┴─────────────────┴────────┴─────────────────┴───
┌─────────────────────────┐   ┌───────────────────────┐   ┌──────────────────────┐
│                         │   │                       │   │                      │
│                         │   │                       │   │                      │
│  Shard 1: Initializing  │   │ Shard 1: Available    │   │  Shard 1: Available  │
│  Shard 2: Initializing  │   │ Shard 2: Available    │   │  Shard 2: Available  │
│  Shard 3: Initializing  │   │ Shard 3: Available    │   │  Shard 3: Available  │
│                         │   │                       │   │                      │
│                         │   │                       │   │                      │
└─────────────────────────┘   └───────────────────────┘   └──────────────────────┘
```

Note that a bootstrap consistency level of `majority` is the default value, but can be modified by changing the value of the key `m3db.client.bootstrap-consistency-level` in [etcd](https://etcd.io/) to one of: `none`, `one`, `unstrict_majority` (attempt to read from majority, but settle for less if any errors occur), `majority` (strict majority), and `all`. For example, if an entire cluster with a replication factor of 3 was restarted simultaneously, all the nodes would get stuck in an infinite loop trying to peer bootstrap from each other and not achieving majority until an operator modified this value. Note that this can happen even if all the shards were in the `Available` state because M3DB nodes will reject all read requests for a shard until they have bootstrapped that shard (which has to happen everytime the node is restarted).

**Note**: Any bootstrappers configuration that does not include the `peers` bootstrapper will be unable to handle dynamic placement changes of any kind.

### Uninitialized Topology Bootstrapper

The purpose of the `uninitialized_topology` bootstrapper is to succeed bootstraps for all time ranges for shards that have never been completely bootstrapped (at a cluster level). This allows us to run the default bootstrapper configuration of: `filesystem,commitlog,peers,uninitialized_topology` such that the `filesystem` and `commitlog` bootstrappers are used by default in node restarts, the `peers` bootstrapper is used for node adds/removes/replaces, and bootstraps still succeed for brand new placement where both the `commitlog` and `peers` bootstrappers will be unable to succeed any bootstraps. In other words, the `uninitialized_topology` bootstrapper allows us to place the `commitlog` bootstrapper *before* the `peers` bootstrapper and still succeed bootstraps with brand new placements without resorting to using the noop-all bootstrapper which suceeds bootstraps for all shard/time-ranges regardless of the status of the placement.

The `uninitialized_topology` bootstrapper determines whether a placement is "new" for a given shard by counting the number of nodes in the `Initializing` state and `Leaving` states and there are more `Initializing` than `Leaving`, then it succeeds the bootstrap because that means the placement has never reached a state where all nodes are `Available`.

### No Operational All Bootstrapper

The `noop-all` bootstrapper succeeds all bootstraps regardless of requests shards/time ranges.

## Bootstrappers Configuration

Now that we've gone over the various bootstrappers, let's consider how M3DB will behave in different configurations. Note that we include `uninitialized_topology` at the end of all the lists of bootstrappers because its required to get a new placement up and running in the first place, but is not required after that (although leaving it in has no detrimental effects). Also note that any configuration that does not include the `peers` bootstrapper will not be able to handle dynamic placement changes like node adds/removes/replaces.

### filesystem,commitlog,peers,uninitialized_topology (default)

This is the default bootstrappers configuration for M3DB and will behave "as expected" in the sense that it will maintain M3DB's consistency guarantees at all times, handle node adds/replaces/removes correctly, and still work with brand new placements / topologies. **This is the only configuration that we recommend using in production**.

In the general case, the node will use only the `filesystem` and `commitlog` bootstrappers on node startup. However, in the case of a node add/remove/replace, the `commitlog` bootstrapper will detect that it is unable to fulfill the bootstrap request (because the node has never reached the `Available` state) and defer to the `peers` bootstrapper to stream in the data.

Additionally, if it is a brand new placement where even the `peers` bootstrapper cannot fulfill the bootstrap, this will be detected by the `uninitialized_topology` bootstrapper which will succeed the bootstrap.

### filesystem,peers,uninitialized_topology

Everytime a node is restarted it will attempt to stream in all of the the data for any blocks that it has never flushed, which is generally the currently active block and possibly the previous block as well. This mode can be useful if you want to improve performance or save disk space by operating nodes without a commitlog, or want to force a repair of any unflushed blocks. This mode can lead to violations of M3DB's consistency guarantees due to the fact that commit logs are being ignored. In addition, if you lose a replication factors worth or more of hosts at the same time, the node will not be able to bootstrap unless an operator modifies the bootstrap consistency level configuration in etcd (see `peers` bootstrap section above). Finally, this mode adds additional network and resource pressure on other nodes in the cluster while one node is peer bootstrapping from them which can be problematic in catastrophic scenarios where all the nodes are trying to stream data from each other.

### peers,uninitialized_topology

Every time a node is restarted, it will attempt to stream in *all* of the data that it is responsible for from its peers, completely ignoring the immutable Fileset files it already has on disk. This mode can be useful if you want to improve performance or save disk space by operating nodes without a commitlog, or want to force a repair of all data on an individual node. This mode can lead to violations of M3DB's consistency guarantees due to the fact that the commit logs are being ignored. In addition, if you lose a replication factors worth or more of hosts at the same time, the node will not be able to bootstrap unless an operator modifies the bootstrap consistency level configuration in etcd (see `peers` bootstrap section above). Finally, this mode adds additional network and resource pressure on other nodes in the cluster while one node is peer bootstrapping from them which can be problematic in catastrophic scenarios where all the nodes are trying to stream data from each other.

## Invalid bootstrappers configuration

For the sake of completeness, we've included a short discussion below of some bootstrapping configurations that we consider "invalid" in that they are likely to lose data / violate M3DB's consistency guarantees and/or not handle placement changes in a correct way.

### filesystem,commitlog,uninitialized_topology

This bootstrapping configuration will work just fine if nodes are never added/replaced/removed, but will fail when attempting a node add/replace/remove.

### filesystem,uninitialized_topology

Every time a node is restarted it will utilize the immutable Fileset files its already written out to disk, but any data that it had received since it wrote out the last set of immutable files will be lost.

### commitlog,uninitialized_topology

Every time a node is restarted it will read all the commit log and snapshot files it has on disk, but it will ignore all the data in the immutable Fileset files that it has already written.

## Crash Recovery

**NOTE:** These steps should not be necessary in most cases, especially if using the default bootstrappers configuration
of `filesystem,commitlog,peers,uninitialized_topology`. However in the case the configuration is non-default or the
cluster has been down for a prolonged period of time these steps may be necessary. A good indicator would be log
messages related to failing to bootstrap from peers due to consistency issues.

M3DB may require manual intervention to recover in the event of a prolonged loss of quorum. This is because the [Peers
Boostrapper](#peers-bootstrapper) must read from a majority of nodes owning a shard to bootstrap.

To relax this bootstrapping constraint, a value stored in etcd must be modified that corresponds to the
`m3db.client.bootstrap-consistency-level` runtime flag. Until the coordinator
[supports](https://github.com/m3db/m3/issues/1959) an API for this, this must be done manually. The M3 contributors are
aware of how cumbersome this is and are working on this API.

To update this value in etcd, first determine the environment the M3DB node is using. For example in [this][cfg-env]
configuration, it is `default_env`. If using the M3DB Operator, the value will be `$KUBE_NAMESPACE/$CLUSTER_NAME`, where
`$KUBE_NAMESPACE` is the name of the Kubernetes namespace the cluster is located in and `$CLUSTER_NAME` is the name you
have assigned the cluster (such as `default/my-test-cluster`).

The following base64-encoded string represents a Protobuf-serialized [message][string-proto] containing the string
`unstrict_majority`: `ChF1bnN0cmljdF9tYWpvcml0eQ==`. Decode this string and place it in the following etcd key, where
`$ENV` is the value determined above:

```
_kv/$ENV/m3db.client.bootstrap-consistency-level
```

Note that on MacOS, `base64` requires the `-D` flag to decode, whereas elsewhere it is likely `-d`. Also note the use of
`echo -n` to ensure removal of newlines if your shell does not support the `<<<STRING` pattern.

Once the cluster is recovered, the value should be deleted from etcd to revert to normal behavior using `etcdctl del`.

Examples:

```shell
# On Linux, using a recent bash, update the value for env=default_env
<<<ChF1bnN0cmljdF9tYWpvcml0eQ== base64 -d | env ETCDCTL_API=3 etcdctl put _kv/default_env/m3db.client.bootstrap-consistency-level

# On Linux, using a limited shell, update the value for env=default_env
echo -n "ChF1bnN0cmljdF9tYWpvcml0eQ==" | base64 -d | env ETCDCTL_API=3 etcdctl put _kv/default_env/m3db.client.bootstrap-consistency-level

# On MacOS, update the value for a cluster "test_cluster" in Kubernetes namespace "m3db"
echo -n "ChF1bnN0cmljdF9tYWpvcml0eQ==" | base64 -D | kubectl exec -i $ETCD_POD -- env ETCDCTL_API=3 etcdctl put _kv/m3db/test_cluster/m3db.client.bootstrap-consistency-level

# Delete the key to restore normal behavior
env ETCDCTL_API=3 etcdctl del _kv/default_env/m3db.client.bootstrap-consistency-level
```

[string-proto]: https://github.com/m3db/m3/blob/61c299dba378432de955dbff31e6c1bdd55272d7/src/cluster/generated/proto/commonpb/common.proto#L40-L42
[cfg-env]: https://github.com/m3db/m3/blob/61c299dba378432de955dbff31e6c1bdd55272d7/src/dbnode/config/m3dbnode-all-config.yml#L266
