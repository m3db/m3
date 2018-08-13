# Prometheus

This document is a getting started guide to integrating M3DB with Prometheus.

## M3 Coordinator configuration

To write to a remote M3DB cluster the simplest configuration is to run `m3coordinator` as a sidecar alongside Prometheus.

Start by downloading the [config template](https://github.com/m3db/m3/blob/master/src/query/config/m3coordinator-cluster-template.yml). Update the `namespaces` and the `client` section for a new cluster to match your cluster's configuration.

You'll need to specify the static IPs or hostnames of your M3DB seed nodes, and the name and retention values of the namespace you set up.  You can leave the namespace storage metrics type as `unaggregated` since it's required by default to have a cluster that receives all Prometheus metrics unaggregated.  In the future you might also want to aggregate and downsample metrics for longer retention, and you can come back and update the config once you've setup those clusters.

It should look something like:

```
listenAddress: 0.0.0.0:7201

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: 0.0.0.0:7203 # until https://github.com/m3db/m3/issues/682 is resolved
  sanitization: prometheus
  samplingRate: 1.0
  extended: none

clusters:
   - namespaces:
# We created a namespace called "default" and had set it to retention "48h".
       - namespace: default
         retention: 48h
         storageMetricsType: unaggregated
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
# We have five M3DB nodes but only three are seed nodes, they are listed here.
                 - M3DB_NODE_01_STATIC_IP_ADDRESS:2379
                 - M3DB_NODE_02_STATIC_IP_ADDRESS:2379
                 - M3DB_NODE_03_STATIC_IP_ADDRESS:2379
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

Now start the process up:

```
m3coordinator -f <config-name.yml>
```

Or, use the docker container:

```
docker pull quay.io/m3/m3coordinator:latest
docker run -p 7201:7201 --name m3coordinator -v <config-name.yml>:/etc/m3coordinator/m3coordinator.yml quay.io/m3/m3coordinator:latest
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
```
