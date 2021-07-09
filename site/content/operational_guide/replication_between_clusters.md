---
title: "Replication between clusters"
weight: 15
---

M3DB clusters can be configured to passively replicate data from other clusters. This feature is most commonly used when operators wish to run two (or more) regional clusters that function independently while passively replicating data from the other cluster in an eventually consistent manner.

The cross-cluster replication feature is built on-top of the [background repairs](/v1.0/docs/operational_guide/repairs) feature. As a result, it has all the same caveats and limitations. Specifically, it does not currently work with clusters that use M3DB's indexing feature and the replication delay between two clusters will be at least (`block size` + `bufferPast`) for data written at the beginning of a block for a given namespace. For use-cases where a large replication delay is unacceptable, the current recommendation is to dual-write to both clusters in parallel and then rely upon the cross-cluster replication feature to repair any discrepancies between the clusters caused by failed dual-writes. This recommendation is likely to change in the future once support for low-latency replication is added to M3DB in the form of commitlog tailing.

While cross-cluster replication is built on top of the background repairs feature, background repairs do not need to be enabled for cross-cluster replication to be enabled. In other words, clusters can be configured such that:

1.  Background repairs (within a cluster) are disabled and replication is also disabled.
2.  Background repairs (within a cluster) are enabled, but replication is disabled.
3.  Background repairs (within a cluster) are disabled, but replication is enabled.
4.  Background repairs (within a cluster) are enabled and replication is also enabled.

## Configuration

**Important**: All M3DB clusters involved in the cross-cluster replication process must be configured such that they have the exact same:

1.  Number of shards
2.  Replication factor
3.  Namespace configuration

The replication feature can be enabled by adding the following configuration to `m3dbnode.yml` under the `db` section:

```yaml
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
```

Note that the `repairEnabled` field in the configuration above is independent of the `enabled` field under the `repairs` section. For example, the example above will enable replication of data from `some-other-cluster` but will not perform background repairs within the cluster the M3DB node belongs to.

However, the following configuration:

```yaml
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
```

would enable both replication of data from `some-other-cluster` as well as background repairs within the cluster that the M3DB node belongs to.
