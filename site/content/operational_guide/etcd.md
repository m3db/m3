---
title: "etcd"
weight: 12
---

## Overview

All components within the M3 stack (e.g. M3 Query, M3DB, M3 Aggregator) leverage `etcd` as its distributed key-value storage. See below for examples of how this `etcd` metadata is used: 

1.  Update cluster configuration in realtime
2.  Manage placements for distributed / sharded tiers like M3DB and M3Aggregator
3.  Perform leader-election in M3Aggregator
4.  Define placements for nodes within a M3DB cluster
5.  Define which shards are tied to a given node within a M3DB cluster 

## Best practices for operating etcd with M3

`M3DB` ships with support for running embedded `etcd` (called `seed nodes`), and while this is convenient for testing and development, we don't recommend running with this setup in production.

Both `M3` and `etcd` are complex distributed systems, and trying to operate both within the same binary is challenging and dangerous for production workloads.

Instead, we recommend running an external `etcd` cluster that is isolated from the `M3` stack so that performing operations like node adds, removes, and replaces are easier.

While M3 relies on `etcd` to provide strong consistency, the operations we use it for are all low-throughput so you should be able to operate a very low maintenance `etcd` cluster. [A 3-node setup for high availability](https://github.com/etcd-io/etcd/blob/v3.3.11/Documentation/faq.md#what-is-failure-tolerance) should be more than sufficient for most workloads.

## Configuring an External etcd Cluster

### M3DB

Most of our documentation demonstrates how to run `M3DB` with embedded etcd nodes. Once you're ready to switch to an external `etcd` cluster, all you need to do is modify the `M3DB` config to remove the `seedNodes` field entirely and then change the `endpoints` under `etcdClusters` to point to your external `etcd` nodes instead of the `M3DB` seed nodes.

For example this portion of the config

```yaml
config:
    service:
        env: default_env
        zone: embedded
        service: m3db
        cacheDir: /var/lib/m3kv
        etcdClusters:
            - zone: embedded
              endpoints:
                  - http://m3db_seed1:2379
                  - http://m3db_seed2:2379
                  - http://m3db_seed3:2379
    seedNodes:
        initialCluster:
            - hostID: m3db_seed1
              endpoint: http://m3db_seed1:2380
            - hostID: m3db_seed2
              endpoint: http://m3db_seed2:2380
            - hostID: m3db_seed3
              endpoint: http://m3db_seed3:2380
```

would become

```yaml
config:
    service:
        env: default_env
        zone: embedded
        service: m3db
        cacheDir: /var/lib/m3kv
        etcdClusters:
            - zone: embedded
              endpoints:
                  - http://external_etcd1:2379
                  - http://external_etcd2:2379
                  - http://external_etcd3:2379
```

**Note**: `M3DB` placements and namespaces are stored in `etcd` so if you want to switch to an external `etcd` cluster you'll need to recreate all your placements and namespaces. You can do this manually or use `etcdctl`'s [Mirror Maker](https://github.com/etcd-io/etcd/blob/v3.3.11/etcdctl/doc/mirror_maker.md) functionality.

### M3Coordinator

`M3Coordinator` does not run embedded `etcd`, so configuring it to use an external `etcd` cluster is simple. Just replace the `endpoints` under `etcdClusters` in the YAML config to point to your external `etcd` nodes instead of the `M3DB` seed nodes. See the `M3DB` example above for a detailed before/after comparison of the YAML config.

## etcd Operations

### Embedded etcd

If you're running `M3DB seed nodes` with embedded `etcd` (which we do not recommend for production workloads) and need to perform a node add/replace/remove then follow our [placement configuration guide](/docs/operational_guide/placement_configuration) and pay special attention to follow the special instructions for `seed nodes`.

### External etcd

Just follow the instructions in the [etcd docs.](https://github.com/etcd-io/etcd/tree/master/Documentation)
