function(coordinator={}, db={}) {
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
        "listenAddress": "0.0.0.0:7203" // until https://github.com/m3db/m3/issues/682 is resolved
      },
      "sanitization": "prometheus",
      "samplingRate": 1.0,
      "extended": "none"
    },
    "limits": {
      "maxComputedDatapoints": 10000
    },
    "tagOptions": {
      // Configuration setting for generating metric IDs from tags.
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
    "listenAddress": "0.0.0.0:9000",
    "clusterListenAddress": "0.0.0.0:9001",
    "httpNodeListenAddress": "0.0.0.0:9002",
    "httpClusterListenAddress": "0.0.0.0:9003",
    "debugListenAddress": "0.0.0.0:9004",
    "hostID": {
      "resolver": "config",
      "value": "m3db_local"
    },
    "client": {
      "writeConsistencyLevel": "majority",
      "readConsistencyLevel": "unstrict_majority"
    },
    "gcPercentage": 100,
    "writeNewSeriesAsync": true,
    "writeNewSeriesLimitPerSecond": 1048576,
    "writeNewSeriesBackoffDuration": "2ms",
    "bootstrap": {
      "bootstrappers": [
        "filesystem",
        "commitlog",
        "peers",
        "uninitialized_topology"
      ],
      "commitlog": {
        "returnUnfulfilledForCorruptCommitLogFiles": false
      }
    },
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
              "127.0.0.1:2379"
            ]
          }
        ]
      },
      "seedNodes": {
        "initialCluster": [
          {
            "hostID": "m3db_local",
            "endpoint": "http://127.0.0.1:2380"
          }
        ]
      }
    }
  } + db,
}
