---
title: "Replication"
date: 2020-04-21T21:01:57-04:00
draft: true
---

Sharding
Timeseries keys are hashed to a fixed set of virtual shards. Virtual shards are then assigned to physical nodes. M3DB can be configured to use any hashing function and a configured number of shards. By default murmur3 is used as the hashing function and 4096 virtual shards are configured.
Benefits
Shards provide a variety of benefits throughout the M3DB stack:
They make horizontal scaling easier and adding / removing nodes without downtime trivial at the cluster level.
They provide more fine grained lock granularity at the memory level.
They inform the filesystem organization in that data belonging to the same shard will be used / dropped together and can be kept in the same file.


Replication
Logical shards are placed per virtual shard per replica with configurable isolation (zone aware, rack aware, etc). For instance, when using rack aware isolation, the set of datacenter racks that locate a replica’s data is distinct to the racks that locate all other replicas’ data.
Replication is synchronization during a write and depending on the consistency level configured will notify the client on whether a write succeeded or failed with respect to the consistency level and replication achieved.
Replica
Each replica has its own assignment of a single logical shard per virtual shard.
Conceptually it can be defined as:
Replica {
  id uint32
  shards []Shard
}

Shard state
Each shard can be conceptually defined as:
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

Shard assignment
The assignment of shards is stored in etcd. When adding, removing or replacing a node shard goal states are assigned for each shard assigned.
For a write to appear as successful for a given replica it must succeed against all assigned hosts for that shard. That means if there is a given shard with a host assigned as LEAVING and another host assigned as INITIALIZING for a given replica writes to both these hosts must appear as successful to return success for a write to that given replica. Currently however only AVAILABLE shards count towards consistency, the work to group the LEAVING and INITIALIZING shards together when calculating a write success/error is not complete, see issue 417.
It is up to the nodes themselves to bootstrap shards when the assignment of new shards to it are discovered in the INITIALIZING state and to transition the state to AVAILABLE once bootstrapped by calling the cluster management APIs when done. Using a compare and set this atomically removes the LEAVING shard still assigned to the node that previously owned it and transitions the shard state on the new node from INITIALIZING state to AVAILABLE.
Nodes will not start serving reads for the new shard until it is AVAILABLE, meaning not until they have bootstrapped data for those shards.



Replication and Deployment in Zones
Overview
M3DB supports both deploying across multiple zones in a region or deploying to a single zone with rack-level isolation. It can also be deployed across multiple regions for a global view of data, though both latency and bandwidth costs may increase as a result.
In addition, M3DB has support for automatically replicating data between isolated M3DB clusters (potentially running in different zones / regions). More details can be found in the Replication between clusters operational guide.
Replication
A replication factor of at least 3 is highly recommended for any M3DB deployment, due to the consistency levels (for both reads and writes) that require quorum in order to complete an operation. For more information on consistency levels, see the documentation concerning tuning availability, consistency and durability.
M3DB will do its best to distribute shards evenly among the availability zones while still taking each individual node's weight into account, but if some of the availability zones have less available hosts than others then each host in that zone will be responsible for more shards than hosts in the other zones and will thus be subjected to heavier load.
Replication Factor Recommendations
Running with RF=1 or RF=2 is not recommended for any multi-node use cases (testing or production). In the future such topologies may be rejected by M3DB entirely. It is also recommended to only run with an odd number of replicas.
RF=1 is not recommended as it is impossible to perform a safe upgrade or tolerate any node failures: as soon as one node is down, all writes destined for the shards it owned will fail. If the node's storage is lost (e.g. the disk fails), the data is gone forever.
RF=2, despite having an extra replica, entails many of the same problems RF=1 does. When M3DB is configured to perform quorum writes and reads (the default), as soon as a single node is down (for planned maintenance or an unplanned disruption) clients will be unable to read or write (as the quorum of 2 nodes is 2). Even if clients relax their consistency guarantees and read from the remaining serving node, users may experience flapping results depending on whether one node had data for a time window that the other did not.
Finally, it is only recommended to run with an odd number of replicas. Because the quorum size of an even-RF N is (N/2)+1, any cluster with an even replica factor N has the same failure tolerance as a cluster with RF=N-1. "Failure tolerance" is defined as the number of isolation groups you can concurrently lose nodes across. The following table demonstrates the quorum size and failure tolerance of various RF's, inspired by etcd's failure tolerance documentation.
Replica Factor
Quorum Size
Failure Tolerance
1
1
0
2
2
0
3
2
1
4
3
1
5
3
2
6
4
2
7
4
3
Upgrading hosts in a deployment
When an M3DB node is restarted it has to perform a bootstrap process before it can serve reads. During this time the node will continue to accept writes, but will not be available for reads.
Obviously, there is also a small window of time during between when the process is stopped and then started again where it will also be unavailable for writes.
Deployment across multiple availability zones in a region
For deployment in a region, it is recommended to set the isolationGroup host attribute to the name of the availability zone a host is in.
In this configuration, shards are distributed among hosts such that each will not be placed more than once in the same availability zone. This allows an entire availability zone to be lost at any given time, as it is guaranteed to only affect one replica of data.
For example, in a multi-zone deployment with four shards spread over three availability zones:

Typically, deployments have many more than four shards - this is a simple example that illustrates how M3DB maintains availability while losing an availability zone, as two of three replicas are still intact.
Deployment in a single zone
For deployment in a single zone, it is recommended to set the isolationGroup host attribute to the name of the rack a host is in or another logical unit that separates groups of hosts in your zone.
In this configuration, shards are distributed among hosts such that each will not be placed more than once in the same defined rack or logical unit. This allows an entire unit to be lost at any given time, as it is guaranteed to only affect one replica of data.
For example, in a single-zone deployment with three shards spread over four racks:

Typically, deployments have many more than three shards - this is a simple example that illustrates how M3DB maintains availability while losing a single rack, as two of three replicas are still intact.
Deployment across multiple regions
For deployment across regions, it is recommended to set the isolationGroup host attribute to the name of the region a host is in.
As mentioned previously, latency and bandwidth costs may increase when using clusters that span regions.
In this configuration, shards are distributed among hosts such that each will not be placed more than once in the same region. This allows an entire region to be lost at any given time, as it is guaranteed to only affect one replica of data.
For example, in a multi-region deployment with four shards spread over five regions:

Typically, deployments have many more than four shards - this is a simple example that illustrates how M3DB maintains availability while losing up to two regions, as three of five replicas are still intact.

Replication between clusters (beta)
Overview
M3DB clusters can be configured to passively replicate data from other clusters. This feature is most commonly used when operators wish to run two (or more) regional clusters that function independently while passively replicating data from the other cluster in an eventually consistent manner.
The cross-cluster replication feature is built on-top of the background repairs feature. As a result, it has all the same caveats and limitations. Specifically, it does not currently work with clusters that use M3DB's indexing feature and the replication delay between two clusters will be at least (block size + bufferPast) for data written at the beginning of a block for a given namespace. For use-cases where a large replication delay is unacceptable, the current recommendation is to dual-write to both clusters in parallel and then rely upon the cross-cluster replication feature to repair any discrepancies between the clusters caused by failed dual-writes. This recommendation is likely to change in the future once support for low-latency replication is added to M3DB in the form of commitlog tailing.
While cross-cluster replication is built on top of the background repairs feature, background repairs do not need to be enabled for cross-cluster replication to be enabled. In other words, clusters can be configured such that:
Background repairs (within a cluster) are disabled and replication is also disabled.
Background repairs (within a cluster) are enabled, but replication is disabled.
Background repairs (within a cluster) are disabled, but replication is enabled.
Background repairs (within a cluster) are enabled and replication is also enabled.
Configuration
Important: All M3DB clusters involved in the cross-cluster replication process must be configured such that they have the exact same:
Number of shards
Replication factor
Namespace configuration
The replication feature can be enabled by adding the following configuration to m3dbnode.yml under the db section:
db:
  ... (other configuration)
  replication:
    clusters:
      - name: "some-other-cluster"
        repairEnabled: true
        client:
          config:
            service:
              env: <ETCD_ENV>
              zone: <ETCD_ZONE>
              service: <ETCD_SERVICE>
              cacheDir: /var/lib/m3kv
              etcdClusters:
                - zone: <ETCD_ZONE>
                  endpoints:
                    - <ETCD_ENDPOINT_01_HOST>:<ETCD_ENDPOINT_01_PORT>

Note that the repairEnabled field in the configuration above is independent of the enabled field under the repairs section. For example, the example above will enable replication of data from some-other-cluster but will not perform background repairs within the cluster the M3DB node belongs to.
However, the following configuration:
db:
  ... (other configuration)
  repair:
    enabled: true

  replication:
    clusters:
      - name: "some-other-cluster"
        repairEnabled: true
        client:
          config:
            service:
              env: <ETCD_ENV>
              zone: <ETCD_ZONE>
              service: <ETCD_SERVICE>
              cacheDir: /var/lib/m3kv
              etcdClusters:
                - zone: <ETCD_ZONE>
                  endpoints:
                    - <ETCD_ENDPOINT_01_HOST>:<ETCD_ENDPOINT_01_PORT>

would enable both replication of data from some-other-cluster as well as background repairs within the cluster that the M3DB node belongs to.
