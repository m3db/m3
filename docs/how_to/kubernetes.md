# M3DB on Kubernetes

M3DB on Kubernetes is currently in the alpha phase of development. We currently provide static manifests to bootstrap a
cluster. In the future we hope to create an [operator](https://coreos.com/operators/) and leverage [custom resource
definitions](https://v1-10.docs.kubernetes.io/docs/concepts/api-extension/custom-resources/) (CRDs) to automatically
handle operations such as managing cluster topology, but for now our manifests should be adequate to get started.

## Prerequisites

M3DB performs better when it has access to fast disks. Every incoming write is written to a commit log, which at high
volumes of writes can be sensitive to spikes in disk latency. Additionally the random seeks into files when loading cold
files benefit from lower random read latency.

Because of this, the included manifests reference a
[StorageClass](https://v1-10.docs.kubernetes.io/docs/concepts/storage/storage-classes/) named `fast`. Manifests are
provided to provision such a StorageClass on AWS / Azure / GCP using the respective cloud provider's premium disk class.

If you do not already have a StorageClass named `fast`, create one using one of the provided manifests:
```
# AWS EBS (class io1)
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db/master/kube/storage-class-aws.yaml

# Azure premium LRS
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db/master/kube/storage-class-azure.yaml

# GCE Persistent SSD
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db/master/kube/storage-class-gcp.yaml
```

If you wish to use your cloud provider's default remote disk, or another disk class entirely, you'll have to modify the
manifests.

## Deploying

Apply the following manifest to create your cluster:
```
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db/master/kube/bundle.yaml
```

Applying this bundle will create the following resources:

1. An `m3db` [Namespace](https://v1-10.docs.kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) for
   all M3DB-related resources.
2. A 3-node etcd cluster in the form of a
   [StatefulSet](https://v1-10.docs.kubernetes.io/docs/concepts/workloads/controllers/statefulset/) backed by persistent
   remote SSDs. This cluster stores the DB topology and other runtime configuration data.
3. A 3-node M3DB cluster in the form of a StatefulSet.
4. [Headless services](https://v1-10.docs.kubernetes.io/docs/concepts/services-networking/dns-pod-service/#services) for
   the etcd and m3db StatefulSets to provide stable DNS hostnames per-pod.

Wait until all created pods are listed as ready:
```
$ kubectl -n m3db get po
NAME         READY     STATUS    RESTARTS   AGE
etcd-0       1/1       Running   0          22m
etcd-1       1/1       Running   0          22m
etcd-2       1/1       Running   0          22m
m3dbnode-0   1/1       Running   0          22m
m3dbnode-1   1/1       Running   0          22m
m3dbnode-2   1/1       Running   0          22m
```

You can now proceed to initialize a namespace and placment for the cluster the same as you would for our other how-to
guides:
```
# Open a local connection to the coordinator service:
$ kubectl -n m3db port-forward svc/m3coordinator 7201
Forwarding from 127.0.0.1:7201 -> 7201
Forwarding from [::1]:7201 -> 7201
```

```json
# Create an initial cluster topology
curl -sSf -X POST localhost:7201/api/v1/placement/init -d '{
    "num_shards": 256,
    "replication_factor": 3,
    "instances": [
        {
            "id": "m3dbnode-0",
            "isolation_group": "pod0",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3dbnode-0.m3dbnode:9000",
            "hostname": "m3dbnode-0.m3dbnode",
            "port": 9000
        },
        {
            "id": "m3dbnode-1",
            "isolation_group": "pod1",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3dbnode-1.m3dbnode:9000",
            "hostname": "m3dbnode-1.m3dbnode",
            "port": 9000
        },
        {
            "id": "m3dbnode-2",
            "isolation_group": "pod2",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3dbnode-2.m3dbnode:9000",
            "hostname": "m3dbnode-2.m3dbnode",
            "port": 9000
        }
    ]
}'
```

```json
# Create a namespace to hold your metrics
curl -X POST localhost:7201/api/v1/namespace/add -d '{
  "name": "metrics",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": false,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodNanos": '"$((1000000000*60*60*24*30))"',
      "blockSizeNanos": '"$((1000000000*60*60*12))"',
      "bufferFutureNanos": '"$((1000000000*60*60*1))"',
      "bufferPastNanos": '"$((1000000000*60*60*1))"'
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeNanos": '"$((1000000000*60*60*12))"'
    }
  }
}'
```

Shortly after you should see your nodes finish bootstrapping:
```
$ kubectl -n m3db logs -f m3dbnode-0
21:36:54.831698[I] cluster database initializing topology
21:36:54.831732[I] cluster database resolving topology
21:37:22.821740[I] resolving namespaces with namespace watch
21:37:22.821813[I] updating database namespaces [{adds [metrics]} {updates []} {removals []}]
21:37:23.008109[I] node tchannelthrift: listening on 0.0.0.0:9000
21:37:23.008384[I] cluster tchannelthrift: listening on 0.0.0.0:9001
21:37:23.217090[I] node httpjson: listening on 0.0.0.0:9002
21:37:23.217240[I] cluster httpjson: listening on 0.0.0.0:9003
21:37:23.217526[I] bootstrapping shards for range starting [{run bootstrap-data} {bootstrapper filesystem} ...
...
21:37:23.239534[I] bootstrap data fetched now initializing shards with series blocks [{namespace metrics} {numShards 256} {numSeries 0}]
21:37:23.240778[I] bootstrap finished [{namespace metrics} {duration 23.325194ms}]
21:37:23.240856[I] bootstrapped
21:37:29.733025[I] successfully updated topology to 3 hosts
```

You can now write and read metrics using the API on the db nodes:
```
$ kubectl -n m3db port-forward svc/m3dbnode 9003
Forwarding from 127.0.0.1:9003 -> 9003
Forwarding from [::1]:9003 -> 9003
```

```json
curl -sSf -X POST localhost:9003/writetagged -d '{
  "namespace": "metrics",
  "id": "foo",
  "tags": [
    {
      "name": "city",
      "value": "new_york"
    },
    {
      "name": "endpoint",
      "value": "/request"
    }
  ],
  "datapoint": {
    "timestamp": '"$(date "+%s")"',
    "value": 42.123456789
  }
}'
```

```json
$ curl -sSf -X POST http://localhost:9003/query -d '{
  "namespace": "metrics",
  "query": {
    "regexp": {
      "field": "city",
      "regexp": ".*"
    }
  },
  "rangeStart": 0,
  "rangeEnd": '"$(date "+%s")"'
}' | jq .

{
  "results": [
    {
      "id": "foo",
      "tags": [
        {
          "name": "city",
          "value": "new_york"
        },
        {
          "name": "endpoint",
          "value": "/request"
        }
      ],
      "datapoints": [
        {
          "timestamp": 1527630053,
          "value": 42.123456789
        }
      ]
    }
  ],
  "exhaustive": true
}
```

### Adding nodes

You can easily scale your M3DB cluster by scaling the StatefulSet and informing the cluster topology of the change:
```
kubectl -n m3db scale --replicas=4 statefulset/m3dbnode
```

Once the pod is ready you can modify the cluster topology:
```
kubectl -n m3db port-forward svc/m3coordinator 7201
Forwarding from 127.0.0.1:7201 -> 7201
Forwarding from [::1]:7201 -> 7201
```

```json
curl -sSf -X POST localhost:7201/api/v1/placement/add -d '{
    "instances": [
        {
            "id": "m3dbnode-3",
            "isolation_group": "pod3",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3dbnode-3.m3dbnode:9000",
            "hostname": "m3dbnode-3.m3dbnode",
            "port": 9000
        }
    ]
}'
```

## Integrations

### Prometheus

As mentioned in our integrations [guide](../integrations/prometheus.md), M3DB can be used as a [remote read/write
endpoint](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#%3Cremote_write%3E) for Prometheus.
If you run Prometheus on your Kubernetes cluster you can easily point it at M3DB in your Prometheus server config:

```
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
```

## Scheduling

In some cases you might prefer M3DB to run on certain nodes in your cluster. For example: if your cluster is comprised
of different instance types and some have more memory than others then you'd like M3DB to run on those nodes if
possible. To accommodate this, the pods created by the StatefulSets use [pod
affinities](https://v1-10.docs.kubernetes.io/docs/concepts/configuration/assign-pod-node/) and
[tolerations](https://v1-10.docs.kubernetes.io/docs/concepts/configuration/taint-and-toleration/) to prefer to run on
certain nodes. Specifically:

1. The pods tolerate the taint `"dedicated-m3db"` to run on nodes that are specifically dedicated to m3db if you so
   choose.
2. Via `nodeAffinity` the pods prefer to run on nodes with the label `m3db.io/dedicated-m3db="true"`.
