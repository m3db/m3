local cluster = {
  HOST1_ETCD_ID: "host1",
  HOST1_ETCD_IP_ADDRESS: "HOST1_STATIC_IP_ADDRESS",
  HOST2_ETCD_ID: "host2",
  HOST2_ETCD_IP_ADDRESS: "HOST2_STATIC_IP_ADDRESS",
  HOST3_ETCD_ID: "host3",
  HOST3_ETCD_IP_ADDRESS: "HOST3_STATIC_IP_ADDRESS",
};

function(cluster, coordinator={}, db={}) {
  "coordinator": {
    "listenAddress": {
      "type": "config",
      "value": "0.0.0.0:7201"
    },
    "local": {
      "namespaces": [
        {
          "namespace": "default",
          "type": "unaggregated",
          "retention": "48h"
        }
      ]
    },
    "logging": {
      "level": "info"
    },
    "metrics": {
      "scope": {
        "prefix": "coordinator"
      },
      "prometheus": {
        "handlerPath": "/metrics",
        "listenAddress": "0.0.0.0:7203"
      },
      "sanitization": "prometheus",
      "samplingRate": 1.0,
      "extended": "none"
    },
    "tagOptions": {
      "idScheme": "quoted"
    }
  } + coordinator,
  "db": {
    "logging": {
      "level": "info"
    },
    "metrics": {
      "prometheus": {
        "handlerPath": "/metrics"
      },
      "sanitization": "prometheus",
      "samplingRate": 1.0,
      "extended": "detailed"
    },
    "hostID": {
      "resolver": "hostname"
    },
    "config": {
      "service": {
        "env": "default_env",
        "zone": "embedded",
        "service": "m3db",
        "cacheDir": "/var/lib/m3kv",
        "etcdClusters": [
          {
            "zone": "embedded",
            "endpoints": [
              "http://"+cluster.HOST1_ETCD_IP_ADDRESS+":2379",
              "http://"+cluster.HOST2_ETCD_IP_ADDRESS+":2379",
              "http://"+cluster.HOST3_ETCD_IP_ADDRESS+":2379"
            ]
          }
        ]
      },
      "seedNodes": {
        "initialCluster": [
          {
            "hostID": cluster.HOST1_ETCD_ID,
            "endpoint": "http://"+cluster.HOST1_ETCD_IP_ADDRESS+":2380"
          },
          {
            "hostID": cluster.HOST2_ETCD_ID,
            "endpoint": "http://"+cluster.HOST2_ETCD_IP_ADDRESS+":2380"
          },
          {
            "hostID": cluster.HOST3_ETCD_ID,
            "endpoint": "http://"+cluster.HOST3_ETCD_IP_ADDRESS+":2380"
          }
        ]
      }
    },
    "listenAddress": "0.0.0.0:9000",
    "clusterListenAddress": "0.0.0.0:9001",
    "httpNodeListenAddress": "0.0.0.0:9002",
    "httpClusterListenAddress": "0.0.0.0:9003",
    "debugListenAddress": "0.0.0.0:9004",
    "client": {
      "writeConsistencyLevel": "majority",
      "readConsistencyLevel": "unstrict_majority"
    },
    "gcPercentage": 100,
    "writeNewSeriesAsync": true,
    "writeNewSeriesBackoffDuration": "2ms",
    "cache": {
      "series": {
        "policy": "lru"
      },
      "postingsList": {
        "size": 262144
      }
    },
    "commitlog": {
      "flushMaxBytes": 524288,
      "flushEvery": "1s",
      "queue": {
        "calculationType": "fixed",
        "size": 2097152
      }
    },
    "fs": {
      "filePathPrefix": "/var/lib/m3db"
    }
  } + db,
}