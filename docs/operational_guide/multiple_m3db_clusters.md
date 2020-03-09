## Write to multiple M3DB clusters via M3Coordinator

### Overview:

Default M3 architecture has the M3Coordinator writing to and aggregating meterics from a single M3DB cluster. To map a single coordinator to more than one M3DB cluster, follow the below instructions. 

Use case(s):
- Sending metrics to different namespaces for different retention periods, etc.

### Instructions: 

**Note:** Adding multiple M3DB clusters to m3coordinator using the m3aggregator requires clusterManagement.

**Note:** When making API requests, an environment header needs to be set. 

1. Add clusterManagement to congfig file:

Example config file with clusterManagement (see end of the config):

```bash
  - namespaces:
       - namespace: 21d
         retention: 504h
         type: unaggregated
     client:
       config:
         service:
           env: default_env
           zone: embedded
           service: m3db
           cacheDir: /data/m3kv_default
           etcdClusters:
             - zone: embedded
               endpoints:
              - <ETCD_IP_1>
              - <ETCD_IP_2>
              - <ETCD_IP_3>
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
   - namespaces:
       - namespace: 90d
         retention: 2160h
         type: aggregated
         resolution: 10m
       - namespace: 500d
         retention: 12000h
         type: aggregated
         resolution: 1h
     client:
       config:
         service:
           env: lts_env
           zone: embedded
           service: m3db
           cacheDir: /data/m3kv_lts
           etcdClusters:
             - zone: embedded
               endpoints:
                 - <ETCD_IP_1>
                 - <ETCD_IP_2>
                 - <ETCD_IP_3>
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
tagOptions:
  idScheme: quoted
clusterManagement:
  etcd:
    env: default_env
    zone: embedded
    service: m3db
    cacheDir: /data/m3kv_default
    etcdClusters:
      - zone: embedded
        endpoints:
          - <ETCD_IP_1>
          - <ETCD_IP_2>
          - <ETCD_IP_3>
```
