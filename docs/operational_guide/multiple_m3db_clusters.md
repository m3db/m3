## Write to multiple M3DB clusters via m3coordinator

### Overview:

Default M3 architecture has the m3coordinator writing to and aggregating metrics from a single M3DB cluster. To map a single coordinator to more than one M3DB cluster, follow the below instructions. 

Use case(s):
- Sending metrics to different namespaces for different retention periods, etc.

### Instructions: 

1. Add `clusterManagement` to config file to add multiple M3BD clusters to m3coordinator:

Example config file with `clusterManagement` (see end of the config):

```bash
clusters:
  # Should match the namespace(s) set up in the DB nodes
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
2. Use the `Cluster-Environment-Name` header for any API requests to the m3coordinator. 