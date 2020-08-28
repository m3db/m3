---
title: "Managing cluster"
date: 2020-05-08T12:46:31-04:00
draft: true
---

Creating a Cluster
Once you've installed the M3DB operator and read over the requirements, you can start creating some M3DB clusters!

Basic Cluster
The following creates an M3DB cluster spread across 3 zones, with each M3DB instance being able to store up to 350gb of data using your Kubernetes cluster's default storage class. For examples of different cluster topologies, such as zonal clusters, see the docs on node affinity.

Etcd
Create an etcd cluster with persistent volumes:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/v0.6.0/example/etcd/etcd-pd.yaml
We recommend modifying the storageClassName in the manifest to one that matches your cloud provider's fastest remote storage option, such as pd-ssd on GCP.

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
Ephemeral Cluster
WARNING: This setup is not intended for production-grade clusters, but rather for "kicking the tires" with the operator and M3DB. It is intended to work across almost any Kubernetes environment, and as such has as few dependencies as possible (namely persistent storage). See below for instructions on creating a more durable cluster.

Etcd
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

Deleting a Cluster
Delete your M3DB cluster with kubectl:

kubectl delete m3dbcluster simple-cluster
By default, the operator will delete the placement and namespaces associated with a cluster before the CRD resource deleted. If you do not want this behavior, set keepEtcdDataOnDelete to true on your cluster spec.

Under the hood, the operator uses Kubernetes finalizers to ensure the cluster CRD is not deleted until the operator has had a chance to do cleanup.

Debugging Stuck Cluster Deletion
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
