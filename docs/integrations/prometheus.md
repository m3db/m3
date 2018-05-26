# Prometheus

This document is a getting started guide to integrating M3DB with Prometheus.

## M3 Coordinator configuration

To write to a remote M3DB cluster the simplest configuration is to run `m3coordinator` as a sidecar alongside Prometheus.

Start by downloading the [config template](https://github.com/m3db/m3db/blob/master/src/coordinator/config/m3coordinator-cluster-template.yml). Update the config ‘service’ and 'seedNodes' sections to match your cluster's configuration, something like:

```
config:
  service:
    env: default_env
    zone: embedded
    service: m3db
    cacheDir: /var/lib/m3kv
    etcdClusters:
      - zone: embedded
        endpoints:
          - SEED_HOST_1_STATIC_IP:2379
          - SEED_HOST_2_STATIC_IP:2379
          ...
  seedNodes:
    initialCluster:
      - hostID: SEED_HOST_1_NAME
        endpoint: http://SEED_HOST_1_STATIC_IP:2380
      - hostID: SEED_HOST_2_NAME
        endpoint: http://SEED_HOST_2_STATIC_IP:2380
      ...
```

Now start the process up:

```
m3coordinator -f <config-name.yml>
```

## Prometheus configuration

Add to your Prometheus configuration the `m3coordinator` sidecar remote read/write endpoints, something like:

```
remote_read:
  - url: "http://localhost:7201/api/v1/prom/remote/read"
    # To test reading even when local Prometheus has the data
    read_recent: true

remote_write:
  - url: "http://localhost:7201/api/v1/prom/remote/write"
    # To differentiate between local and remote storage we will add a storage label
    write_relabel_configs:
      - target_label: metrics_storage
        replacement: m3db_remote
```
