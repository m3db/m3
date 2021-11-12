---
title: "Requirements"
menuTitle: "Requirements"
weight: 10
chapter: true
---

## Kubernetes Versions

The M3DB operator current targets Kubernetes 1.11 and 1.12. Given the operator's current production use cases at Uber,
we typically target the two most recent minor Kubernetes versions supported by GKE. We welcome community contributions
to support more recent versions while meeting the aforementioned GKE targets!

## Multi-Zone Kubernetes Cluster

The M3DB operator is intended to be used with Kubernetes clusters that span at least 3 zones within a region to create
highly available clusters and maintain quorum in the event of region failures. Instructions for creating regional
clusters on GKE can be found [here][gke-regional].

## Etcd

M3DB stores its cluster topology and all other runtime metadata in [etcd][etcd].

For *testing / non-production use cases*, we provide simple manifests for running etcd on Kubernetes in our [example
manifests][etcd-example]: one for running ephemeral etcd containers and one for running etcd using basic persistent
volumes. If using the `etcd-pd` yaml manifest, we recommend a modification to use a `StorageClass` equivalent to your
cloud provider's fastest remote disk (such as `pd-ssd` on GCP).

For production use cases, we recommend running etcd (in order of preference):

1. External to your Kubernetes cluster to avoid circular dependencies.
2. Using the [etcd operator][etcd-operator].

[etcd]: https://etcd.io
[etcd-example]: https://github.com/m3db/m3db-operator/tree/master/example/etcd
[etcd-operator]: https://github.com/coreos/etcd-operator
[gke-regional]: https://cloud.google.com/kubernetes-engine/docs/concepts/regional-clusters
