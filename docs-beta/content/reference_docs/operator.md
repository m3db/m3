---
title: "M3Operator"
date: 2020-04-21T21:02:41-04:00
draft: true
---

### Requirements
#### Kubernetes Versions
The M3DB operator current targets Kubernetes 1.11 and 1.12. Given the operator's current production use cases at Uber, we typically target the two most recent minor Kubernetes versions supported by GKE. We welcome community contributions to support more recent versions while meeting the aforementioned GKE targets!

#### Multi-Zone Kubernetes Cluster
The M3DB operator is intended to be used with Kubernetes clusters that span at least 3 zones within a region to create highly available clusters and maintain quorum in the event of region failures. Instructions for creating regional clusters on GKE can be found here.

#### Etcd
M3DB stores its cluster topology and all other runtime metadata in etcd.

For testing / non-production use cases, we provide simple manifests for running etcd on Kubernetes in our example manifests: one for running ephemeral etcd containers and one for running etcd using basic persistent volumes. If using the etcd-pd yaml manifest, we recommend a modification to use a StorageClass equivalent to your cloud provider's fastest remote disk (such as pd-ssd on GCP).

For production use cases, we recommend running etcd (in order of preference):

External to your Kubernetes cluster to avoid circular dependencies.
Using the etcd operator.


### Introduction
Welcome to the documentation for the M3DB operator, a Kubernetes operator for running the open-source timeseries database M3DB on Kubernetes.

Please note that this is alpha software, and as such its APIs and behavior are subject to breaking changes. While we aim to produce thoroughly tested reliable software there may be undiscovered bugs.

For more background on the M3DB operator, see our KubeCon keynote on its origins and usage at Uber.

#### Philosophy
The M3DB operator aims to automate everyday tasks around managing M3DB. Specifically, it aims to automate:

Creating M3DB clusters
Destroying M3DB clusters
Expanding clusters (adding instances)
Shrinking clusters (removing instances)
Replacing failed instances
It explicitly does not try to automate every single edge case a user may ever run into. For example, it does not aim to automate disaster recovery if an entire cluster is taken down. Such use cases may still require human intervention, but the operator will aim to not conflict with such operations a human may have to take on a cluster.

Generally speaking, the operator's philosophy is if it would be unclear to a human what action to take, we will not try to guess.

#### Installation
Be sure to take a look at the requirements before installing the operator.

#### Helm
Add the m3db-operator repo:
helm repo add m3db https://m3-helm-charts.storage.googleapis.com/stable
Install the m3db-operator chart:
helm install m3db/m3db-operator --namespace m3db-operator
Note: If uninstalling an instance of the operator that was installed with Helm, some resources such as the ClusterRole, ClusterRoleBinding, and ServiceAccount may need to be deleted manually.

#### Manually
Install the bundled operator manifests in the current namespace:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/bundle.yaml 

#### Creating a Cluster
Once you've installed the M3DB operator and read over the requirements, you can start creating some M3DB clusters!

#### Basic Cluster
The following creates an M3DB cluster spread across 3 zones, with each M3DB instance being able to store up to 350gb of data using your Kubernetes cluster's default storage class. For examples of different cluster topologies, such as zonal clusters, see the docs on node affinity.

#### Etcd
Create an etcd cluster with persistent volumes:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/v0.6.0/example/etcd/etcd-pd.yaml
We recommend modifying the storageClassName in the manifest to one that matches your cloud provider's fastest remote storage option, such as pd-ssd on GCP.

### 
M3DB
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
#### Ephemeral Cluster
WARNING: This setup is not intended for production-grade clusters, but rather for "kicking the tires" with the operator and M3DB. It is intended to work across almost any Kubernetes environment, and as such has as few dependencies as possible (namely persistent storage). See below for instructions on creating a more durable cluster.

### Etcd
Create an etcd cluster in the same namespace your M3DB cluster will be created in. If you don't have persistent storage available, this will create a cluster that will not use persistent storage and will likely become unavailable if any of the pods die:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/v0.6.0/example/etcd/etcd-basic.yaml

# Verify etcd health once pods available
kubectl exec etcd-0 -- env ETCDCTL_API=3 etcdctl endpoint health
# 127.0.0.1:2379 is healthy: successfully committed proposal: took = 2.94668ms
If you have remote storage available and would like to jump straight to using it, apply the following manifest for etcd instead:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/v0.6.0/example/etcd/etcd-pd.yaml
M3DB
Once etcd is available, you can create an M3DB cluster. An example of a very basic M3DB cluster definition is as follows:

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
metadata:
  name: simple-cluster
spec:
  image: quay.io/m3db/m3dbnode:latest
  replicationFactor: 3
  numberOfShards: 256
  etcdEndpoints:
  - http://etcd-0.etcd:2379
  - http://etcd-1.etcd:2379
  - http://etcd-2.etcd:2379
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
  podIdentityConfig:
    sources:
      - PodUID
  namespaces:
    - name: metrics-10s:2d
      preset: 10s:2d
This will create a highly available cluster with RF=3 spread evenly across the three given zones within a region. A pod's UID will be used for its identity. The cluster will have 1 namespace that stores metrics for 2 days at 10s resolution.

Next, apply your manifest:

$ kubectl apply -f example/simple-cluster.yaml
m3dbcluster.operator.m3db.io/simple-cluster created
Shortly after all pods are created you should see the cluster ready!

$ kubectl get po -l operator.m3db.io/app=m3db
NAME                    READY   STATUS    RESTARTS   AGE
simple-cluster-rep0-0   1/1     Running   0          1m
simple-cluster-rep1-0   1/1     Running   0          56s
simple-cluster-rep2-0   1/1     Running   0          37s
We can verify that the cluster has finished streaming data by peers by checking that an instance has bootstrapped:

$ kubectl exec simple-cluster-rep2-0 -- curl -sSf localhost:9002/health
{"ok":true,"status":"up","bootstrapped":true}


### Deleting a Cluster
Delete your M3DB cluster with kubectl:

kubectl delete m3dbcluster simple-cluster
By default, the operator will delete the placement and namespaces associated with a cluster before the CRD resource deleted. If you do not want this behavior, set keepEtcdDataOnDelete to true on your cluster spec.

Under the hood, the operator uses Kubernetes finalizers to ensure the cluster CRD is not deleted until the operator has had a chance to do cleanup.

### Debugging Stuck Cluster Deletion
If for some reason the operator is unable to delete the placement and namespace for the cluster, the cluster CRD itself will be stuck in a state where it can not be deleted, due to the way finalizers work in Kubernetes. The operator might be unable to clean up the data for many reasons, for example if the M3DB cluster itself is not available to serve the APIs for cleanup or if etcd is down and cannot fulfill the deleted.

To allow the CRD to be deleted, you can kubectl edit m3dbcluster $CLUSTER and remove the operator.m3db.io/etcd-deletion finalizer. For example, in the following cluster you'd remove the finalizer from metadata.finalizers:

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
metadata:
  ...
  finalizers:
  - operator.m3db.io/etcd-deletion
  name: m3db-cluster
...
Note that if you do this, you'll have to manually remove the relevant data in etcd. For a cluster in namespace $NS with name $CLUSTER, the keys are:

_sd.placement/$NS/$CLUSTER/m3db
_kv/$NS/$CLUSTER/m3db.node.namespaces

### Monitoring
M3DB exposes metrics via a Prometheus endpoint. If using the Prometheus Operator, you can apply a ServiceMonitor to have your M3DB pods automatically scraped by Prometheus:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/example/prometheus-servicemonitor.yaml

### Configuring M3DB
By default the operator will apply a configmap with basic M3DB options and settings for the coordinator to direct Prometheus reads/writes to the cluster. This template can be found here.

To apply custom a configuration for the M3DB cluster, one can set the configMapName parameter of the cluster spec to an existing configmap.

### Environment Warning
If providing a custom config map, the env you specify in your config must be $NAMESPACE/$NAME, where $NAMESPACE is the Kubernetes namespace your cluster is in and $NAME is the name of the cluster. For example, with the following cluster:

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
metadata:
  name: cluster-a
  namespace: production
...
The value of env in your config MUST be production/cluster-a. This restriction allows multiple M3DB clusters to safely share the same etcd cluster.

