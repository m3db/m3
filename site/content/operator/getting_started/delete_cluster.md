---
title: "Deleting a Cluster"
menuTitle: "Deleting a Cluster"
weight: 14
chapter: true
---

Delete your M3DB cluster with `kubectl`:
```
kubectl delete m3dbcluster simple-cluster
```

By default, the operator will delete the placement and namespaces associated with a cluster before the CRD resource
deleted. If you do **not** want this behavior, set `keepEtcdDataOnDelete` to `true` on your cluster spec.

Under the hood, the operator uses Kubernetes [finalizers] to ensure the cluster CRD is not deleted until the operator
has had a chance to do cleanup.

## Debugging Stuck Cluster Deletion

If for some reason the operator is unable to delete the placement and namespace for the cluster, the cluster CRD itself
will be stuck in a state where it can not be deleted, due to the way finalizers work in Kubernetes. The operator might
be unable to clean up the data for many reasons, for example if the M3DB cluster itself is not available to serve the
APIs for cleanup or if etcd is down and cannot fulfill the deleted.

To allow the CRD to be deleted, you can `kubectl edit m3dbcluster $CLUSTER` and remove the
`operator.m3db.io/etcd-deletion` finalizer. For example, in the following cluster you'd remove the finalizer from `metadata.finalizers`:

```yaml
apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
metadata:
  ...
  finalizers:
  - operator.m3db.io/etcd-deletion
  name: m3db-cluster
...
```

Note that if you do this, you'll have to manually remove the relevant data in etcd. For a cluster in namespace `$NS`
with name `$CLUSTER`, the keys are:

- `_sd.placement/$NS/$CLUSTER/m3db`
- `_kv/$NS/$CLUSTER/m3db.node.namespaces`

[finalizers]: https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#finalizers
