# Setting up m3query

## Configuration

Before setting up m3query, make sure that you have at least one m3db node running. In order to start m3query, you need to configure a `yaml` file, that will be used to connect to m3db. Below is a sample config file that is used for an embedded etcd cluster.

```json
listenAddress:
  type: "config"
  value: "0.0.0.0:7201"

metrics:
  scope:
    prefix: "query"
  prometheus:
    handlerPath: /metrics
    listenAddress: 0.0.0.0:7203 # until https://github.com/m3db/m3/issues/682 is resolved
  sanitization: prometheus
  samplingRate: 1.0
  extended: none

clusters:
  - namespaces:
      - namespace: default
        type: unaggregated
        retention: 48h
      - namespace: metrics_10s_48h
        type: aggregated
        retention: 48h
        resolution: 10s
    client:
      config:
        service:
          env: default_env
          zone: embedded
          service: m3db
          cacheDir: /var/lib/m3kv
          etcdClusters:
            - zone: embedded
              endpoints:
                - 127.0.0.1:2379
        seedNodes:
          initialCluster:
            - hostID: m3db_local
              endpoint: http://127.0.0.1:2380
      writeConsistencyLevel: majority
      readConsistencyLevel: unstrict_majority
      writeTimeout: 10s
      fetchTimeout: 15s
      connectTimeout: 20s
      writeRetry:
        initialBackoff: 500ms
        backoffFactor: 3
        maxRetries: 2
        jitter: true
      fetchRetry:
        initialBackoff: 500ms
        backoffFactor: 2
        maxRetries: 3
        jitter: true
      backgroundHealthCheckFailLimit: 4
      backgroundHealthCheckFailThrottleFactor: 0.5
```

## Aggregation

You will notice that in the setup above, m3db has two namespaces configured (1 aggregated and 1 unaggregated). It it important to note that all writes go to all namespaces so as long as you include all namespaces in your query config, you will be querying all namespaces. Additionally, if you run Statsite, m3agg, or some other aggregation tier, you will want to set the `all` flag under `downsample` to false since there is no need for the query engine to aggregate already aggregated metrics. For example,

```json
- namespace: metrics_10s_48h
  type: aggregated
  retention: 48h
  resolution: 10s
  downsample:
    all: false
```

## Grafana

You can also set up m3query as a datasource in Grafana. To do this, add a new datasource with a type of `Prometheus`. The URL should point to the host/port running m3query.
