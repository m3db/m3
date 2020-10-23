---
title: Using the Kubernetes Operator
weight: 3
---
<!-- TODO: Most of this is much the same as the quickstart, maybe combine, or make this the next start? -->
[The Kubernetes operator](https://operator.m3db.io/) helps you deploy M3 to a cluster. It is a more streamlined setup that uses [custom resource definitions](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) to automatically handle operations such as managing cluster placements.

{{% notice warning %}}
The Operator is **alpha software**, and as such its APIs and behavior are subject to breaking changes. While we
aim to produce thoroughly tested reliable software there may be undiscovered bugs.
{{% /notice %}}

The M3 operator aims to automate everyday tasks around managing M3. Specifically, it aims to automate:

-   Creating M3 clusters
-   Destroying M3 clusters
-   Expanding clusters (adding instances)
-   Shrinking clusters (removing instances)
-   Replacing failed instances

It does not try to automate every single edge case a user may ever run into. For example, it does not aim to
automate disaster recovery if an entire cluster is taken down. Such use cases may still require human intervention, but
the operator aims to not conflict with such operations a human may have to take on a cluster.

Generally speaking, the operator's philosophy is if **it would be unclear to a human what action to take, we try not to guess.**

For more background on the operator, see our [KubeCon keynote](https://kccna18.sched.com/event/Gsxn/keynote-smooth-operator-large-scale-automated-storage-with-kubernetes-celina-ward-software-engineer-matt-schallert-site-reliability-engineer-uber) on its origins and usage at Uber.

## Prerequisites

### Kubernetes Versions

The M3 operator targets Kubernetes 1.11 and 1.12, we typically target the two most recent minor Kubernetes versions supported by GKE.

### Multi-Zone Kubernetes Cluster

The M3 operator is intended for use with Kubernetes clusters that span at least 3 zones within a region to create
highly available clusters and maintain quorum in the event of region failures. Instructions for creating regional
clusters on GKE can be found [here](https://cloud.google.com/kubernetes-engine/docs/concepts/regional-clusters).

### An etcd cluster

M3DB stores its cluster placements and all other runtime metadata in [etcd](https://etcd.io).

For testing or non-production use cases, we provide manifests for running etcd on Kubernetes in our [example manifests](https://github.com/m3db/m3db-operator/tree/master/example/etcd). There is one for running ephemeral etcd containers and one for running etcd using persistent volumes.

For production use cases, we recommend running etcd the following ways (in order of preference):

1.  External to your Kubernetes cluster to avoid circular dependencies.
2.  Using the [etcd operator](https://github.com/coreos/etcd-operator).

## Installation

### Helm

1.  Add the `m3db-operator` repo:

    ```shell
    helm repo add m3db https://m3-helm-charts.storage.googleapis.com/stable
    ```

2.  Install the `m3db-operator` chart:

    ```shell
    helm install m3db/m3db-operator --namespace m3db-operator
    ```

{{% notice note %}}
If you're uninstalling an instance of the operator previously installed with Helm, you may need to delete some resources such as the ClusterRole, ClusterRoleBinding, and ServiceAccount manually.
{{% /notice %}}

### Manually

Install the bundled operator manifests in the current namespace:

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/bundle.yaml
```

## Create an M3 Cluster

The M3 operator and Kubernetes allows for many different cluster configurations, and this guide presents two examples with details on where to find further information on the settings they use and why.


This example creates an M3 cluster spread across 3 zones, with each instance able to store up to 350gb of data using the Kubernetes cluster's default storage class. It creates three isolated groups for nodes, each with one node instance. In a production environment [you can use a variety of different options to define](https://operator.m3db.io/configuration/node_affinity/) how Kubernetes spreads nodes across groups based on factors such as resource capacity, or location.

### Create an etcd Cluster

Create an etcd cluster with persistent volumes, using [our example manifest file](https://raw.githubusercontent.com/m3db/m3db-operator/v0.9.0/example/etcd/etcd-pd.yaml):

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/v0.9.0/example/etcd/etcd-pd.yaml
```

{{% notice tip %}}
We recommend modifying the `storageClassName` in the manifest above to one that matches your cloud provider's fastest remote storage option, such as `pd-ssd` on Google cloud.
{{% /notice %}}

### Create an M3 Cluster


Focussing on M3 specific aspects of the manifest below inside the `spec` object, it creates a cluster with 3 replicas of data across 256 shards that connects to the 3 available etcd endpoints.

It creates three `isolationGroups` for nodes, each with one node instance. In a production environment you can use a variety of different options to define how nodes are spread across groups based on factors such as resource capacity, or location.

It creates namespaces in the cluster with the `namespaces` parameter. You can use [M3-provided presets](https://operator.m3db.io/configuration/namespaces/#presets), or define your own. This example creates a namespace with the `10s:2d` preset, which means the cluster stores metrics at 10 second resolution for 2 days. 




The cluster derives pod identity from the `podIdentityConfig` parameter. Setting no sources defaults the value to `PodName`.

`dataDirVolumeClaimTemplate` is the volume claim template for an instance's data. It claims `PersistentVolumes` for cluster storage, volumes are dynamically provisioned  when the `StorageClass` is defined.



[Read more details on all the parameters in the Operator API docs](https://operator.m3db.io/api/).




```yaml
apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
metadata:
  name: persistent-cluster
spec:
  image: quay.io/m3db/m3dbnode:latest
  replicationFactor: 3
  numberOfShards: 256
  isolationGroups:
  - name: group1
    numInstances: 1
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - <zone-a>
  - name: group2
    numInstances: 1
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - <zone-b>
  - name: group3
    numInstances: 1
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - <zone-c>
  etcdEndpoints:
  - http://etcd-0.etcd:2379
  - http://etcd-1.etcd:2379
  - http://etcd-2.etcd:2379
  podIdentityConfig:
    sources: []
  namespaces:
    - name: metrics-10s:2d
      preset: 10s:2d
  dataDirVolumeClaimTemplate:
    metadata:
      name: m3db-data
    spec:
      accessModes:
      - ReadWriteOnce
      resources:
        requests:
          storage: 350Gi
        limits:
          storage: 350Gi
```