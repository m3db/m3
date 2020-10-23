---
title: Using the Kubernetes Operator to Update a Cluster
weight: 3
---


If you need to make changes to a running cluster, you can use the operator and add an annotation to enable updates.

First update the M3 cluster's [custom resource definition](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) (CRD), this is any Kubernetes configuration file beginning with the following:

```yaml
apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
```

And then apply the changes with `kubectl`:

```shell
kubectl apply -f {cluster-config.yaml}
```

As a precaution, the operator doesn't immediately update a cluster after you apply changes. Instead, you need to add the following annotation on each `StatefulSet` in the cluster to show to the operator that it is safe to update that `StatefulSet`:

```bash
kubectl annotate statefulset {cluster-name} operator.m3db.io/update=enabled
```

When the operator sees this annotation, it checks if the current state of the `StatefulSet` differs from its desired state as defined by the `M3DBCluster` CRD. If so, the operator updates the `StatefulSet` to match the desired state, thereby triggering a rollout of the pods in the `StatefulSet`. The operator also removes the `operator.m3db.io/update=enabled` annotation from the updated `StatefulSet`.

If the operator finds the update annotation on a `StatefulSet` that doesn't need updating, then the operator removes the annotation and performs no other actions.

Once you set the update annotation on a `StatefulSet`, you can watch for the removal of the annotation to see if the operator has seen and checked for an update.

Since M3 rollouts can take a long time, it's often more convenient to set the annotation to enable updates on each `StatefulSet` in the cluster at once, and allow the operator to perform the rollout safely.

The operator updates one `StatefulSet` at a time and then waits for it to bootstrap and become healthy again before moving onto the next `StatefulSet` in the
cluster so that no two replicas are ever down at the same time.

```bash
kubectl annotate statefulset -l operator.m3db.io/cluster={cluster-name} operator.m3db.io/update=enabled
```
