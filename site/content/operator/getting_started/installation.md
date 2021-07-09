---
title: "Installation"
menuTitle: "Installation"
weight: 11
chapter: true
---

Be sure to take a look at the [requirements](/v1.0/docs/operator/getting_started/requirements) before installing the operator.

## Helm

1. Add the `m3db-operator` repo:

```
helm repo add m3db https://m3-helm-charts.storage.googleapis.com/stable
```

2. Install the `m3db-operator` chart:

```
helm install m3db-operator m3db/m3db-operator
```

**Note**: If uninstalling an instance of the operator that was installed with Helm, some resources such as the
ClusterRole, ClusterRoleBinding, and ServiceAccount may need to be deleted manually.


## Manually

Install the bundled operator manifests in the current namespace:

```
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/bundle.yaml
```
