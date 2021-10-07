---
title: "Running M3 Coordinator in Admin mode"
---

Sometimes it is useful to run M3 Coordinator in "admin mode". Usually it is enough o have a single dedicated instance that is used to perform various administration tasks:
- M3DB placement management
- M3 Aggregator placement management
- Namespace operations

To run M3 Coordinator in admin mode simply start it with `noop-etcd` backend:

```yaml
backend: noop-etcd
```

Usually for production clusters we run external Etcd. Set it up under `clusterManagement` configuration key.

Final configuration might look as follows:

```yaml
listenAddress: 0.0.0.0:7201

metrics:
  scope:
    prefix: "coordinator-admin"
  prometheus:
    handlerPath: /metrics
    listenAddress: 0.0.0.0:3030
  sanitization: prometheus
  samplingRate: 1.0

backend: noop-etcd

clusterManagement:
  etcd:
    env: default_env
    zone: embedded
    service: m3db
    cacheDir: /var/lib/m3kv
    etcdClusters:
    - zone: embedded
      endpoints:
      - etcd01:2379
```