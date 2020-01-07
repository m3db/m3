# M3DB Bundle Kubernetes Terraform Module

This folder contains the main module for setting up an M3DB cluster on Kubernetes which includes:

1. An M3DB [Namespace](https://v1-10.docs.kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) for all M3DB-related resources.
1. A 3-node etcd cluster in the form of a [StatefulSet](https://v1-10.docs.kubernetes.io/docs/concepts/workloads/controllers/statefulset/) backed by persistent remote SSDs. This cluster stores the DB topology and other runtime configuration data.
1. A 3-node M3DB cluster in the form of a StatefulSet.
1. [Headless services](https://v1-10.docs.kubernetes.io/docs/concepts/services-networking/dns-pod-service/#services) for the etcd and m3db StatefulSets to provide stable DNS hostnames per-pod.

Public cloud provider specific instructions can be found in the examples subfolder.

There are a couple of properties that currently exist in the raw bundle yaml file which has not yet been implemented by Terraform. These will be updated once the features exist in Terraform:

```yaml
tolerations:
  - key: "dedicated-m3db"
    effect: NoSchedule
    operator: Exist

```
A [pull request](https://github.com/terraform-providers/terraform-provider-kubernetes/pull/246) already exists for tolerations, but has yet to be merged.


```yaml
affinity:
  nodeAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - preference:
        matchExpressions:
          - key: m3db.io/dedicated-m3db
            operator: In
            values:
              - "true"
        weight: 10

```
Node Affinity is being tracked by [issue #233](https://github.com/terraform-providers/terraform-provider-kubernetes/issues/233) and is currently being worked on.

