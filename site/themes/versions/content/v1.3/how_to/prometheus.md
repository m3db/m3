---
title: "Integrating with Prometheus"
date: 2020-04-21T20:48:58-04:00
draft: true
---

### Prometheus
As mentioned in our integrations guide, M3DB can be used as a remote read/write endpoint for Prometheus.
If you run Prometheus on your Kubernetes cluster you can easily point it at M3DB in your Prometheus server config:

remote_read:
  - url: "http://m3coordinator.m3db.svc.cluster.local:7201/api/v1/prom/remote/read"
    # To test reading even when local Prometheus has the data
    read_recent: true

remote_write:
  - url: "http://m3coordinator.m3db.svc.cluster.local:7201/api/v1/prom/remote/write"
    # To differentiate between local and remote storage we will add a storage label
    write_relabel_configs:
      - target_label: metrics_storage
        replacement: m3db_remote


### M3 Coordinator configuration
To write to a remote M3DB cluster the simplest configuration is to run m3coordinator as a sidecar alongside Prometheus.

Start by downloading the config template. Update the namespaces and the client section for a new cluster to match your cluster's configuration.

You'll need to specify the static IPs or hostnames of your M3DB seed nodes, and the name and retention values of the namespace you set up. You can leave the namespace storage metrics type as unaggregated since it's required by default to have a cluster that receives all Prometheus metrics unaggregated. In the future you might also want to aggregate and downsample metrics for longer retention, and you can come back and update the config once you've setup those clusters. You can read more about our aggregation functionality here.

It should look something like:
```yaml
listenAddress: 0.0.0.0:7201

logging:
  level: info

metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: 0.0.0.0:7203 # until https://github.com/m3db/m3/issues/682 is resolved
  sanitization: prometheus
  samplingRate: 1.0
  extended: none

tagOptions:
  idScheme: quoted

clusters:
   - namespaces:
# We created a namespace called "default" and had set it to retention "48h".
       - namespace: default
         retention: 48h
         type: unaggregated
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
`m3coordinator -f <config-name.yml>`

Or, use the docker container:
```
docker pull quay.io/m3db/m3coordinator:latest
docker run -p 7201:7201 --name m3coordinator -v <config-name.yml>:/etc/m3coordinator/m3coordinator.yml quay.io/m3db/m3coordinator:latest
```

### Prometheus configuration
Add to your Prometheus configuration the m3coordinator sidecar remote read/write endpoints, something like:
```
remote_read:
  - url: "{{% apiendpoint %}}prom/remote/read"
    # To test reading even when local Prometheus has the data
    read_recent: true
remote_write:
  - url: "{{% apiendpoint %}}prom/remote/write"
```

Also, we recommend adding M3DB and M3Coordinator/M3Query to your list of jobs under scrape_configs so that you can monitor them using Prometheus. With this scraping setup, you can also use our pre-configured M3DB Grafana dashboard.
```
- job_name: 'm3db'
  static_configs:
    - targets: ['<M3DB_HOST_NAME_1>:7203', '<M3DB_HOST_NAME_2>:7203', '<M3DB_HOST_NAME_3>:7203']
- job_name: 'm3coordinator'
  static_configs:
    - targets: ['<M3COORDINATOR_HOST_NAME_1>:7203']
```

NOTE: If you are running M3DB with embedded M3Coordinator, you should only have one job. We recommend just calling this job m3. For example:
```
- job_name: 'm3'
  static_configs:
    - targets: ['<HOST_NAME>:7203']
```
