resource "kubernetes_namespace" "m3db_namespace" {
  metadata {
    name = "m3db"
  }
}

resource "kubernetes_service" "etcd_service" {
  metadata {
    name      = "etcd"
    namespace = "m3db"
    labels {
      app = "etcd"
    }
  }
  spec {
    port {
      name = "client"
      port = 2379
    }
    port {
      name = "peer"
      port = 2380
    }
    selector {
      app = "etcd"
    }
    cluster_ip = "None"
  }
}

resource "kubernetes_service" "etcd_cluster" {
  metadata {
    name      = "etcd-cluster"
    namespace = "m3db"
    labels {
      app = "etcd"
    }
  }
  spec {
    port {
      protocol = "TCP"
      port     = 2379
    }
    selector {
      app = "etcd"
    }
    type = "ClusterIP"
  }
}

resource "kubernetes_stateful_set" "etcd_stateful_set" {
  metadata {
    name      = "etcd"
    namespace = "m3db"
    labels {
      app = "etcd"
    }
  }
  spec {
    service_name = "etcd"
    replicas = 3
    selector {
      match_labels {
        app = "etcd"
      }
    }
    template {
      metadata {
        labels {
          app = "etcd"
        }
      }
      spec {
        container {
          name    = "etcd"
          image   = "quay.io/coreos/etcd:v3.4.3"
          command = ["etcd", "--name", "$(MY_POD_NAME)", "--listen-peer-urls", "http://$(MY_IP):2380", "--listen-client-urls", "http://$(MY_IP):2379,http://127.0.0.1:2379", "--advertise-client-urls", "http://$(MY_POD_NAME).etcd:2379", "--initial-cluster-token", "etcd-cluster-1", "--initial-advertise-peer-urls", "http://$(MY_POD_NAME).etcd:2380", "--initial-cluster", "etcd-0=http://etcd-0.etcd:2380,etcd-1=http://etcd-1.etcd:2380,etcd-2=http://etcd-2.etcd:2380", "--initial-cluster-state", "new", "--data-dir", "/var/lib/etcd"]
          port {
            name           = "client"
            container_port = 2379
          }
          port {
            name           = "peer"
            container_port = 2380
          }
          env {
            name = "MY_IP"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }
          env {
            name = "MY_POD_NAME"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }
          env {
            name  = "ETCDCTL_API"
            value = "3"
          }
          volume_mount {
            name       = "etcd-data"
            mount_path = "/var/lib/etcd"
          }
        }
      }
    }
    volume_claim_template {
      metadata {
        name = "etcd-data"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        resources {
          requests {
            storage = "50Gi"
          }
        }
        storage_class_name = "fast"
      }
    }
  }
}

resource "kubernetes_config_map" "m3dbnode_config" {
  metadata {
    name      = "m3dbnode-config"
    namespace = "m3db"
  }
  data {
    m3dbnode.yml = "coordinator:\n  listenAddress: \"0.0.0.0:7201\"\n  metrics:\n    scope:\n      prefix: \"coordinator\"\n    prometheus:\n      handlerPath: /metrics\n      listenAddress: 0.0.0.0:7203\n    sanitization: prometheus\n    samplingRate: 1.0\n    extended: none\n  tagOptions:\n    idScheme: quoted\n\ndb:\n  logging:\n    level: info\n\n  metrics:\n    prometheus:\n      handlerPath: /metrics\n    sanitization: prometheus\n    samplingRate: 1.0\n    extended: detailed\n\n  listenAddress: 0.0.0.0:9000\n  clusterListenAddress: 0.0.0.0:9001\n  httpNodeListenAddress: 0.0.0.0:9002\n  httpClusterListenAddress: 0.0.0.0:9003\n  debugListenAddress: 0.0.0.0:9004\n\n  hostID:\n    resolver: hostname\n\n  client:\n    writeConsistencyLevel: majority\n    readConsistencyLevel: unstrict_majority\n\n  gcPercentage: 100\n\n  writeNewSeriesAsync: true\n  writeNewSeriesBackoffDuration: 2ms\n\n  commitlog:\n    flushMaxBytes: 524288\n    flushEvery: 1s\n    queue:\n      calculationType: fixed\n      size: 2097152\n\n  filesystem:\n    filePathPrefix: /var/lib/m3db\n\n  config:\n    service:\n        env: default_env\n        zone: embedded\n        service: m3db\n        cacheDir: /var/lib/m3kv\n        etcdClusters:\n        - zone: embedded\n          endpoints:\n          - http://etcd-0.etcd:2379\n          - http://etcd-1.etcd:2379\n          - http://etcd-2.etcd:2379\n"
  }
}

resource "kubernetes_service" "m3dbnode_service" {
  metadata {
    name      = "m3dbnode"
    namespace = "m3db"
    labels {
      app = "m3dbnode"
    }
  }
  spec {
    port {
      name = "client"
      port = 9000
    }
    port {
      name = "cluster"
      port = 9001
    }
    port {
      name = "http-node"
      port = 9002
    }
    port {
      name = "http-cluster"
      port = 9003
    }
    port {
      name = "debug"
      port = 9004
    }
    port {
      name = "coordinator"
      port = 7201
    }
    port {
      name = "coordinator-metrics"
      port = 7203
    }
    selector {
      app = "m3dbnode"
    }
    cluster_ip = "None"
  }
}

resource "kubernetes_service" "m3_coordinator_service" {
  metadata {
    name      = "m3coordinator"
    namespace = "m3db"
    labels {
      app = "m3dbnode"
    }
  }
  spec {
    port {
      name = "coordinator"
      port = 7201
    }
    port {
      name = "coordinator-metrics"
      port = 7203
    }
    selector {
      app = "m3dbnode"
    }
  }
}

resource "kubernetes_stateful_set" "m3dbnode_stateful_set" {
  metadata {
    name      = "m3dbnode"
    namespace = "m3db"
    labels {
      app = "m3dbnode"
    }
  }
  spec {
    service_name = "m3dbnode"
    replicas = 3
    selector {
      match_labels {
        app = "m3dbnode"
      }
    }
    update_strategy {
      type = "RollingUpdate"
    }
    template {
      metadata {
        labels {
          app = "m3dbnode"
        }
      }
      spec {
        container {
          name  = "m3db"
          image = "quay.io/m3/m3dbnode:latest"
          image_pull_policy = "Always"
          args  = ["-f", "/etc/m3db/m3dbnode.yml"]
          volume_mount {
            name       = "config-vol"
            mount_path = "/etc/m3db/"
          }
          volume_mount {
            name       = "m3db-db"
            mount_path = "/var/lib/m3db"
          }
          port {
            name           = "client"
            container_port = 9000
            protocol       = "TCP"
          }
          port {
            name           = "cluster"
            container_port = 9001
            protocol       = "TCP"
          }
          port {
            name           = "http-node"
            container_port = 9002
            protocol       = "TCP"
          }
          port {
            name           = "http-cluster"
            container_port = 9003
            protocol       = "TCP"
          }
          port {
            name           = "debug"
            container_port = 9004
            protocol       = "TCP"
          }
          port {
            name           = "coordinator"
            container_port = 7201
            protocol       = "TCP"
          }
          port {
            name           = "coord-metrics"
            container_port = 7203
            protocol       = "TCP"
          }
        }
        volume {
          name = "config-vol"
          config_map {
            name = "m3dbnode-config"
          }
        }
        dns_policy                       = "ClusterFirst"
        restart_policy                   = "Always"
        termination_grace_period_seconds = 30
      }
    }
    volume_claim_template {
      metadata {
        name = "m3db-db"
      }
      spec {
        access_modes = ["ReadWriteOnce"]
        storage_class_name = "fast"
        resources {
          requests {
            storage = "200Gi"
          }
        }
      }
    }
  }
}

