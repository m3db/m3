---
title: "Namespaces"
menuTitle: "Namespaces"
weight: 12
chapter: true
---

M3DB uses the concept of [namespaces][m3db-namespaces] to determine how metrics are stored and retained. The M3DB
operator allows a user to define their own namespaces, or to use a set of presets we consider to be suitable for
production use cases.

Namespaces are configured as part of an `m3dbcluster` [spec][api-namespaces].

## Presets

### `10s:2d`

This preset will store metrics at 10 second resolution for 2 days. For example, in your cluster spec:

```yaml
spec:
...
  namespaces:
  - name: metrics-short-term
    preset: 10s:2d
```

### `1m:40d`

This preset will store metrics at 1 minute resolution for 40 days.

```yaml
spec:
...
  namespaces:
  - name: metrics-long-term
    preset: 1m:40d
```

## Custom Namespaces

You can also define your own custom namespaces by setting the `NamespaceOptions` within a cluster spec. The
[API][api-ns-options] lists all available fields. As an example, a namespace to store 7 days of data may look like:
```yaml
...
spec:
...
  namespaces:
  - name: custom-7d
    options:
      bootstrapEnabled: true
      flushEnabled: true
      writesToCommitLog: true
      cleanupEnabled: true
      snapshotEnabled: true
      repairEnabled: false
      retentionOptions:
        retentionPeriod: 168h
        blockSize: 12h
        bufferFuture: 20m
        bufferPast: 20m
        blockDataExpiry: true
        blockDataExpiryAfterNotAccessPeriod: 5m
      indexOptions:
        enabled: true
        blockSize: 12h
```


[api-namespaces]: /docs/operator/api#namespace
[api-ns-options]: /docs/operator/api#namespaceoptions
[m3db-namespaces]: https://docs.m3db.io/operational_guide/namespace_configuration/
