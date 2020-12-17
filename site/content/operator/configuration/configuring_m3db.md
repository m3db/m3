---
title: "Configuring M3DB"
menuTitle: "Configuring M3DB"
weight: 10
chapter: true
---

By default the operator will apply a configmap with basic M3DB options and settings for the coordinator to direct
Prometheus reads/writes to the cluster. This template can be found
[here](https://github.com/m3db/m3db-operator/blob/master/assets/default-config.tmpl).

To apply custom a configuration for the M3DB cluster, one can set the `configMapName` parameter of the cluster [spec] to
an existing configmap.

## Environment Warning

If providing a custom config map, the `env` you specify in your [config][config] **must** be `$NAMESPACE/$NAME`, where
`$NAMESPACE` is the Kubernetes namespace your cluster is in and `$NAME` is the name of the cluster. For example, with
the following cluster:

```yaml
apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
metadata:
  name: cluster-a
  namespace: production
...
```

The value of `env` in your config **MUST** be `production/cluster-a`. This restriction allows multiple M3DB clusters to
safely share the same etcd cluster.

[spec]: /docs/operator/api
[config]: https://github.com/m3db/m3db-operator/blob/795973f3329437ced3ac942da440810cd0865235/assets/default-config.yaml#L77
