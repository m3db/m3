---
title: "Node Endpoint"
menuTitle: "Node Endpoint"
weight: 14
chapter: true
---

M3DB stores an [`endpoint`][proto] field on placement instances that is used for communication between DB nodes and from
other components such as the coordinator.

The operator allows customizing the format of this endpoint by setting the `nodeEndpointFormat` field on a cluster spec.
The format of this field uses [Go templates], with the following template fields currently supported:

| Field           | Description |
| -----           | ----------- |
| `PodName`       | Name of the pod |
| `M3DBService`   | Name of the generated M3DB service |
| `PodNamespace`  | Namespace the pod is in |
| `Port`          | Port M3DB is serving RPCs on |

The default format is:
```
{{ .PodName }}.{{ .M3DBService }}:{{ .Port }}
```

As an example of an override, to expose an M3DB cluster to containers in other Kubernetes namespaces `nodeEndpointFormat` can be set to:
```
{{ .PodName }}.{{ .M3DBService }}.{{ .PodNamespace }}:{{ .Port }}
```

[proto]: https://github.com/m3db/m3/blob/9b1dc3051a17620c0a983d60057a9a8c115af9d4/src/cluster/generated/proto/placementpb/placement.proto#L47
[Go templates]: https://golang.org/pkg/text/template/
