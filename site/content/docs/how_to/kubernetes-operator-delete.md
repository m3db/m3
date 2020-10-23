---
title: Using the Kubernetes Operator to Delete a Cluster
weight: 3
---

Delete an M3 cluster using `kubectl`:

```shell
kubectl delete m3dbcluster {cluster-name}
```

By default, the operator deletes the placement and namespaces associated with a cluster before deleting the [custom resource definition](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) (CRD), resources. If you do **not** want this behavior, set `keepEtcdDataOnDelete` to `true` in the cluster configuration.

The operator uses Kubernetes [https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#finalizers] to ensure the cluster CRD is not deleted until the operator has had a chance to do cleanup.

## Debugging Stuck Cluster Deletion

If for some reason the operator is unable to delete the placement and namespace for the cluster, the cluster CRD becomes stuck in a state where Kubernetes can not delete it, due to the way finalizers work.

The operator might be unable to clean up the data for many reasons, for example if the M3 cluster itself is not available to serve the APIs for cleanup or if etcd is down and cannot fulfill the delete request.

To allow the CRD to be deleted, you can `kubectl edit m3dbcluster $CLUSTER` and remove the `operator.m3db.io/etcd-deletion` finalizer.

For example, the following removes the finalizer from `metadata.finalizers`:

```yaml
apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
metadata:
  ...
  finalizers:
  - operator.m3db.io/etcd-deletion
  name: {cluster-name}
...
```

{{% notice note %}}
You have to manually remove the relevant data in etcd. For a cluster in namespace `$NS`
with name `$CLUSTER`, the keys are:

- `_sd.placement/$NS/$CLUSTER/m3db`
- `_kv/$NS/$CLUSTER/m3db.node.namespaces`
{{% notice tip %}}
