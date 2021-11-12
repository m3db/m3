---
title: "API"
menuTitle: "API"
weight: 4
chapter: true
---

This document enumerates the Custom Resource Definitions used by the M3DB Operator. It is auto-generated from code comments.

## Table of Contents
* [ClusterCondition](#clustercondition)
* [ClusterSpec](#clusterspec)
* [ExternalCoordinatorConfig](#externalcoordinatorconfig)
* [IsolationGroup](#isolationgroup)
* [M3DBCluster](#m3dbcluster)
* [M3DBClusterList](#m3dbclusterlist)
* [M3DBStatus](#m3dbstatus)
* [NodeAffinityTerm](#nodeaffinityterm)
* [AggregatedAttributes](#aggregatedattributes)
* [Aggregation](#aggregation)
* [AggregationOptions](#aggregationoptions)
* [DownsampleOptions](#downsampleoptions)
* [IndexOptions](#indexoptions)
* [Namespace](#namespace)
* [NamespaceOptions](#namespaceoptions)
* [RetentionOptions](#retentionoptions)
* [PodIdentity](#podidentity)
* [PodIdentityConfig](#podidentityconfig)

## ClusterCondition

ClusterCondition represents various conditions the cluster can be in.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| type | Type of cluster condition. | ClusterConditionType | false |
| status | Status of the condition (True, False, Unknown). | corev1.ConditionStatus | false |
| lastUpdateTime | Last time this condition was updated. | string | false |
| lastTransitionTime | Last time this condition transitioned from one status to another. | string | false |
| reason | Reason this condition last changed. | string | false |
| message | Human-friendly message about this condition. | string | false |

[Back to TOC](#table-of-contents)

## ClusterSpec

ClusterSpec defines the desired state for a M3 cluster to be converge to.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| image | Image specifies which docker image to use with the cluster | string | false |
| replicationFactor | ReplicationFactor defines how many replicas | int32 | false |
| numberOfShards | NumberOfShards defines how many shards in total | int32 | false |
| isolationGroups | IsolationGroups specifies a map of key-value pairs. Defines which isolation groups to deploy persistent volumes for data nodes | [][IsolationGroup](#isolationgroup) | false |
| namespaces | Namespaces specifies the namespaces this cluster will hold. | [][Namespace](#namespace) | false |
| etcdEndpoints | EtcdEndpoints defines the etcd endpoints to use for service discovery. Must be set if no custom configmap is defined. If set, etcd endpoints will be templated in to the default configmap template. | []string | false |
| keepEtcdDataOnDelete | KeepEtcdDataOnDelete determines whether the operator will remove cluster metadata (placement + namespaces) in etcd when the cluster is deleted. Unless true, etcd data will be cleared when the cluster is deleted. | bool | false |
| enableCarbonIngester | EnableCarbonIngester enables the listener port for the carbon ingester | bool | false |
| configMapName | ConfigMapName specifies the ConfigMap to use for this cluster. If unset a default configmap with template variables for etcd endpoints will be used. See \"Configuring M3DB\" in the docs for more. | *string | false |
| podIdentityConfig | PodIdentityConfig sets the configuration for pod identity. If unset only pod name and UID will be used. | *PodIdentityConfig | false |
| containerResources | Resources defines memory / cpu constraints for each container in the cluster. | [corev1.ResourceRequirements](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.20/#resourcerequirements-v1-core) | false |
| dataDirVolumeClaimTemplate | DataDirVolumeClaimTemplate is the volume claim template for an M3DB instance's data. It claims PersistentVolumes for cluster storage, volumes are dynamically provisioned by when the StorageClass is defined. | *[corev1.PersistentVolumeClaim](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.20/#persistentvolumeclaim-v1-core) | false |
| podSecurityContext | PodSecurityContext allows the user to specify an optional security context for pods. | *corev1.PodSecurityContext | false |
| securityContext | SecurityContext allows the user to specify a container-level security context. | *corev1.SecurityContext | false |
| imagePullSecrets | ImagePullSecrets will be added to every pod. | [][corev1.LocalObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.20/#localobjectreference-v1-core) | false |
| envVars | EnvVars defines custom environment variables to be passed to M3DB containers. | []corev1.EnvVar | false |
| labels | Labels sets the base labels that will be applied to resources created by the cluster. // TODO(schallert): design doc on labeling scheme. | map[string]string | false |
| annotations | Annotations sets the base annotations that will be applied to resources created by the cluster. | map[string]string | false |
| tolerations | Tolerations sets the tolerations that will be applied to all M3DB pods. | []corev1.Toleration | false |
| priorityClassName | PriorityClassName sets the priority class for all M3DB pods. | string | false |
| nodeEndpointFormat | NodeEndpointFormat allows overriding of the endpoint used for a node in the M3DB placement. Defaults to \"{{ .PodName }}.{{ .M3DBService }}:{{ .Port }}\". Useful if access to the cluster from other namespaces is desired. See \"Node Endpoint\" docs for full variables available. | string | false |
| hostNetwork | HostNetwork indicates whether M3DB pods should run in the same network namespace as the node its on. This option should be used sparingly due to security concerns outlined in the linked documentation. https://kubernetes.io/docs/concepts/policy/pod-security-policy/#host-namespaces | bool | false |
| dnsPolicy | DNSPolicy allows the user to set the pod's DNSPolicy. This is often used in conjunction with HostNetwork.+optional | *corev1.DNSPolicy | false |
| externalCoordinator | Specify a \"controlling\" coordinator for the cluster. | *[ExternalCoordinatorConfig](#externalcoordinatorconfig) | false |
| initContainers | Custom setup for db nodes can be done via initContainers Provide the complete spec for the initContainer here If any storage volumes are needed in the initContainer see InitVolumes below | []corev1.Container | false |
| initVolumes | If the InitContainers require any storage volumes Provide the complete specification for the required Volumes here | []corev1.Volume | false |
| podMetadata | PodMetadata is for any Metadata that is unique to the pods, and does not belong on any other objects, such as Prometheus scrape tags | [metav1.ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.20/#objectmeta-v1-meta) | false |
| parallelPodManagement | ParallelPodManagement sets StatefulSets created by the operator to have Parallel pod management instead of OrderedReady. If nil, this will default to true. | *bool | true |
| serviceAccountName | To use a non-default service account, specify the name here otherwise the service account \"default\" will be used. This is useful for advanced use-cases such as pod security policies. The service account must exist. This operator will not create it. | string | false |
| frozen | Frozen is used to stop the operator from taking any further actions on a cluster. This is useful when troubleshooting as it guarantees the operator won't make any changes to the cluster. | bool | false |

[Back to TOC](#table-of-contents)

## ExternalCoordinatorConfig

ExternalCoordinatorConfig defines parameters for using an external coordinator to control the cluster.\n\n- It is expected that there is a separate standalone coordinator cluster. - It is externally managed - not managed by this operator. - It is expected to have a service endpoint.\n\nSetup this db cluster, but do not assume a co-located coordinator. Instead provide a selector here so we can point to a separate coordinator service.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| selector |  | map[string]string | true |
| serviceEndpoint |  | string | false |

[Back to TOC](#table-of-contents)

## IsolationGroup

IsolationGroup defines the name of zone as well attributes for the zone configuration

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| name | Name is the value that will be used in StatefulSet labels, pod labels, and M3DB placement \"isolationGroup\" fields. | string | true |
| nodeAffinityTerms | NodeAffinityTerms is an array of NodeAffinityTerm requirements, which are ANDed together to indicate what nodes an isolation group can be assigned to. | [][NodeAffinityTerm](#nodeaffinityterm) | false |
| numInstances | NumInstances defines the number of instances. | int32 | true |
| storageClassName | StorageClassName is the name of the StorageClass to use for this isolation group. This allows ensuring that PVs will be created in the same zone as the pinned statefulset on Kubernetes < 1.12 (when topology aware volume scheduling was introduced). Only has effect if the clusters `dataDirVolumeClaimTemplate` is non-nil. If set, the volume claim template will have its storageClassName field overridden per-isolationgroup. If unset the storageClassName of the volumeClaimTemplate will be used. | string | false |

[Back to TOC](#table-of-contents)

## M3DBCluster

M3DBCluster defines the cluster

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| metadata |  | [metav1.ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.20/#objectmeta-v1-meta) | false |
| type |  | string | true |
| spec |  | [ClusterSpec](#clusterspec) | true |
| status |  | [M3DBStatus](#m3dbstatus) | false |

[Back to TOC](#table-of-contents)

## M3DBClusterList

M3DBClusterList represents a list of M3DB Clusters

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| metadata |  | [metav1.ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.20/#listmeta-v1-meta) | false |
| items |  | [][M3DBCluster](#m3dbcluster) | true |

[Back to TOC](#table-of-contents)

## M3DBStatus

M3DBStatus contains the current state the M3DB cluster along with a human readable message

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| state | State is a enum of green, yellow, and red denoting the health of the cluster | M3DBState | false |
| conditions | Various conditions about the cluster. | [][ClusterCondition](#clustercondition) | false |
| message | Message is a human readable message indicating why the cluster is in it's current state | string | false |
| observedGeneration | ObservedGeneration is the last generation of the cluster the controller observed. Kubernetes will automatically increment metadata.Generation every time the cluster spec is changed. | int64 | false |

[Back to TOC](#table-of-contents)

## NodeAffinityTerm

NodeAffinityTerm represents a node label and a set of label values, any of which can be matched to assign a pod to a node.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| key | Key is the label of the node. | string | true |
| values | Values is an array of values, any of which a node can have for a pod to be assigned to it. | []string | true |

[Back to TOC](#table-of-contents)

## AggregatedAttributes

AggregatedAttributes are attributes specifying how data points are aggregated.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| resolution | Resolution is the time range to aggregate data across. | string | false |
| downsampleOptions | DownsampleOptions stores options for downsampling data points. | *[DownsampleOptions](#downsampleoptions) | false |

[Back to TOC](#table-of-contents)

## Aggregation

Aggregation describes data points within a namespace.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| aggregated | Aggregated indicates whether data points are aggregated or not. | bool | false |
| attributes | Attributes defines how data is aggregated when Aggregated is set to true. This field is ignored when aggregated is false. | [AggregatedAttributes](#aggregatedattributes) | false |

[Back to TOC](#table-of-contents)

## AggregationOptions

AggregationOptions is a set of options for aggregating data within the namespace.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| aggregations | Aggregations are the aggregations for a namespace. | [][Aggregation](#aggregation) | false |

[Back to TOC](#table-of-contents)

## DownsampleOptions

DownsampleOptions is a set of options related to downsampling data.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| all | All indicates whether to send data points to this namespace. If set to false, this namespace will not receive data points. In this case, data will need to be sent to the namespace via another mechanism (e.g. rollup/recording rules). | bool | false |

[Back to TOC](#table-of-contents)

## IndexOptions

IndexOptions defines parameters for indexing.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| enabled | Enabled controls whether metric indexing is enabled. | bool | false |
| blockSize | BlockSize controls the index block size. | string | false |

[Back to TOC](#table-of-contents)

## Namespace

Namespace defines an M3DB namespace or points to a preset M3DB namespace.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| name | Name is the namespace name. | string | false |
| preset | Preset indicates preset namespace options. | string | false |
| options | Options points to optional custom namespace configuration. | *[NamespaceOptions](#namespaceoptions) | false |

[Back to TOC](#table-of-contents)

## NamespaceOptions

NamespaceOptions defines parameters for an M3DB namespace. See https://m3db.github.io/m3/operational_guide/namespace_configuration/ for more details.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| bootstrapEnabled | BootstrapEnabled control if bootstrapping is enabled. | bool | false |
| flushEnabled | FlushEnabled controls whether flushing is enabled. | bool | false |
| writesToCommitLog | WritesToCommitLog controls whether commit log writes are enabled. | bool | false |
| cleanupEnabled | CleanupEnabled controls whether cleanups are enabled. | bool | false |
| repairEnabled | RepairEnabled controls whether repairs are enabled. | bool | false |
| snapshotEnabled | SnapshotEnabled controls whether snapshotting is enabled. | bool | false |
| retentionOptions | RetentionOptions sets the retention parameters. | [RetentionOptions](#retentionoptions) | false |
| indexOptions | IndexOptions sets the indexing parameters. | [IndexOptions](#indexoptions) | false |
| coldWritesEnabled | ColdWritesEnabled controls whether cold writes are enabled. | bool | false |
| aggregationOptions | AggregationOptions sets the aggregation parameters. | [AggregationOptions](#aggregationoptions) | false |

[Back to TOC](#table-of-contents)

## RetentionOptions

RetentionOptions defines parameters for data retention.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| retentionPeriod | RetentionPeriod controls how long data for the namespace is retained. | string | false |
| blockSize | BlockSize controls the block size for the namespace. | string | false |
| bufferFuture | BufferFuture controls how far in the future metrics can be written. | string | false |
| bufferPast | BufferPast controls how far in the past metrics can be written. | string | false |
| blockDataExpiry | BlockDataExpiry controls the block expiry. | bool | false |
| blockDataExpiryAfterNotAccessPeriod | BlockDataExpiry controls the not after access period for expiration. | string | false |

[Back to TOC](#table-of-contents)

## PodIdentity

PodIdentity contains all the fields that may be used to identify a pod's identity in the M3DB placement. Any non-empty fields will be used to identity uniqueness of a pod for the purpose of M3DB replace operations.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| name |  | string | false |
| uid |  | string | false |
| nodeName |  | string | false |
| nodeExternalID |  | string | false |
| nodeProviderID |  | string | false |

[Back to TOC](#table-of-contents)

## PodIdentityConfig

PodIdentityConfig contains cluster-level configuration for deriving pod identity.

| Field | Description | Scheme | Required |
| ----- | ----------- | ------ | -------- |
| sources | Sources enumerates the sources from which to derive pod identity. Note that a pod's name will always be used. If empty, defaults to pod name and UID. | []PodIdentitySource | true |

[Back to TOC](#table-of-contents)
