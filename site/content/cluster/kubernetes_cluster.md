---
linktitle: "Kubernetes"
title: Creating an M3 Cluster with Kubernetes
weight: 1
---

This guide shows you how to create an M3 cluster of 3 nodes, designed to run locally on the same machine. It is designed to show you how M3 and Kubernetes can work together, but not as a production example.

{{% notice note %}}
This guide assumes you have read the [quickstart](/docs/quickstart/docker), and builds upon the concepts in that guide.
{{% /notice %}}

{{% notice tip %}}
We recommend you use [our Kubernetes operator](/docs/operator/operator) to deploy M3 to a cluster. It is a more streamlined setup that uses [custom resource definitions](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) to automatically handle operations such as managing cluster placements.
{{% /notice %}}

## Prerequisites

-   A running Kubernetes cluster.
    -   For local testing, you can use [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/), [Docker desktop](https://www.docker.com/products/docker-desktop), or [we have a script](https://raw.githubusercontent.com/m3db/m3db-operator/master/scripts/kind-create-cluster.sh) you can use to start a 3 node cluster with [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/).

## Create An etcd Cluster

M3 stores its cluster placements and runtime metadata in [etcd](https://etcd.io) and needs a running cluster to communicate with.

We have example services and stateful sets you can use, but feel free to use your own configuration and change any later instructions accordingly.

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/example/etcd/etcd-minikube.yaml
```

If the etcd cluster is running on your local machine, update your _/etc/hosts_ file to match the domains specified in the `etcd` `--initial-cluster` argument. For example to match the `StatefulSet` declaration in the _etcd-minikube.yaml_ above, that is:

```text
$(minikube ip) etcd-0.etcd
$(minikube ip) etcd-1.etcd
$(minikube ip) etcd-2.etcd
```

Verify that the cluster is running with something like the Kubernetes dashboard, or the command below:

```shell
kubectl exec etcd-0 -- env ETCDCTL_API=3 etcdctl endpoint health
```

## Install the Operator

Install the bundled operator manifests in the current namespace:

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/bundle.yaml
```

## Create an M3 Cluster

The following creates an M3 cluster with 3 replicas of data across 256 shards that connects to the 3 available etcd endpoints.

It creates three isolated groups for nodes, each with one node instance. In a production environment you can use a variety of different options to define how nodes are spread across groups based on factors such as resource capacity, or location.

It creates namespaces in the cluster with the `namespaces` parameter. You can use M3-provided presets, or define your own. This example creates a namespace with the `10s:2d` preset.

The cluster derives pod identity from the `podIdentityConfig` parameter, which in this case is the UID of the Pod.

[Read more details on all the parameters in the Operator API docs](https://operator.m3db.io/api/).

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/example/m3db-local.yaml
```

Verify that the cluster is running with something like the Kubernetes dashboard, or the command below:

```shell
kubectl exec simple-cluster-rep2-0 -- curl -sSf localhost:9002/health
```

## Deleting a Cluster

Delete the M3 cluster using kubectl:

```shell
kubectl delete m3dbcluster simple-cluster
```

By default, the operator uses [finalizers](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#finalizers) to delete the placement and namespaces associated with a cluster before the custom resources. If you do not want this behavior, set `keepEtcdDataOnDelete` to `true` in the cluster configuration.

{{< fileinclude file="cluster-common-steps.md" >}}