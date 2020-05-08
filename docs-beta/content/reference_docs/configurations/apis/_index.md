---
title: "Apis"
date: 2020-05-08T12:41:49-04:00
draft: true
---

### M3 Coordinator, API for reading/writing metrics and M3 management
M3 Coordinator is a service that coordinates reads and writes between upstream systems, such as Prometheus, and downstream systems, such as M3DB.
It also provides management APIs to setup and configure different parts of M3.
The coordinator is generally a bridge for read and writing different types of metrics formats and a management layer for M3.

### API
The M3 Coordinator implements the Prometheus Remote Read and Write HTTP endpoints, they also can be used however as general purpose metrics write and read APIs. Any metrics that are written to the remote write API can be queried using PromQL through the query APIs as well as being able to be read back by the Prometheus Remote Read endpoint.

### Remote Write
Write a Prometheus Remote write query to M3.
URL
/api/v1/prom/remote/write
Method
POST
URL Params
None.
Header Params
Optional
M3-Metrics-Type:
If this header is set, it determines what type of metric to store this metric value as. Otherwise by default, metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting downsample options to all: false for a namespace in the coordinator config, for more see disabling automatic aggregation.

Must be one of:
unaggregated: Write metrics directly to configured unaggregated namespace.
aggregated: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the M3-Storage-Policy header to be set to resolve which namespace to write metrics to.


### M3-Storage-Policy:
If this header is set, it determines which aggregated namespace to read/write metrics directly to/from (bypassing any aggregation).
The value of the header must be in the format of resolution:retention in duration shorthand. e.g. 1m:48h specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

Here is an example of querying metrics from a specific namespace.

### Data Params
Binary snappy compressed Prometheus WriteRequest protobuf message.
Available Tuning Params
Refer here for an up to date list of remote tuning parameters.

#### Sample Call
There isn't a straightforward way to Snappy compress and marshal a Prometheus WriteRequest protobuf message using just shell, so this example uses a specific command line utility instead.
This sample call is made using promremotecli which is a command line tool that uses a Go client to Prometheus Remote endpoints. For more information visit the GitHub repository.
There is also a Java client that can be used to make requests to the endpoint.
Each -t parameter specifies a label (dimension) to add to the metric.
The -h parameter can be used as many times as necessary to add headers to the outgoing request in the form of "Header-Name: HeaderValue".

Here is an example of writing the datapoint at the current unix timestamp with value 123.456:
docker run -it --rm                                            \
  quay.io/m3db/prometheus_remote_client_golang:latest          \
  -u http://host.docker.internal:7201/api/v1/prom/remote/write \
  -t __name__:http_requests_total                              \
  -t code:200                                                  \
  -t handler:graph                                             \
  -t method:get                                                \
  -d $(date +"%s"),123.456
promremotecli_log 2019/06/25 04:13:56 writing datapoint [2019-06-25 04:13:55 +0000 UTC 123.456]
promremotecli_log 2019/06/25 04:13:56 labelled [[__name__ http_requests_total] [code 200] [handler graph] [method get]]
promremotecli_log 2019/06/25 04:13:56 writing to http://host.docker.internal:7201/api/v1/prom/remote/write
{"success":true,"statusCode":200}
promremotecli_log 2019/06/25 04:13:56 write success

# If you are paranoid about image tags being hijacked/replaced with nefarious code, you can use this SHA256 tag:
# quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b

For more details on querying data in PromQL that was written using this endpoint, see the query API documentation.
Remote Read
Read Prometheus metrics from M3.
URL
/api/v1/prom/remote/read
Method
POST
URL Params
None.
Header Params
Optional

### M3-Metrics-Type:
If this header is set, it determines what type of metric to store this metric value as. Otherwise by default, metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting downsample options to all: false for a namespace in the coordinator config, for more see disabling automatic aggregation.

#### Must be one of:
unaggregated: Write metrics directly to configured unaggregated namespace.
aggregated: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the M3-Storage-Policy header to be set to resolve which namespace to write metrics to.


### M3-Storage-Policy:
If this header is set, it determines which aggregated namespace to read/write metrics directly to/from (bypassing any aggregation).
The value of the header must be in the format of resolution:retention in duration shorthand. e.g. 1m:48h specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

Here is an example of querying metrics from a specific namespace.
Data Params
Binary snappy compressed Prometheus WriteRequest protobuf message.

### Query Engine

API
Please note: This documentation is a work in progress and more detail is required.
Query using PromQL
Query using PromQL and returns JSON datapoints compatible with the Prometheus Grafana plugin.
URL
/api/v1/query_range
Method
GET
URL Params
Required
start=[time in RFC3339Nano]
end=[time in RFC3339Nano]
step=[time duration]
target=[string]
Optional
debug=[bool]
lookback=[string|time duration]: This sets the per request lookback duration to something other than the default set in config, can either be a time duration or the string "step" which sets the lookback to the same as the step request parameter.
Header Params
Optional

### M3-Metrics-Type:
If this header is set, it determines what type of metric to store this metric value as. Otherwise by default, metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting downsample options to all: false for a namespace in the coordinator config, for more see disabling automatic aggregation.

#### Must be one of:
unaggregated: Write metrics directly to configured unaggregated namespace.
aggregated: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the M3-Storage-Policy header to be set to resolve which namespace to write metrics to.


### M3-Storage-Policy:
If this header is set, it determines which aggregated namespace to read/write metrics directly to/from (bypassing any aggregation).
The value of the header must be in the format of resolution:retention in duration shorthand. e.g. 1m:48h specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

#### Here is an example of querying metrics from a specific namespace.
Tag Mutation
The M3-Map-Tags-JSON header enables dynamically mutating tags in Prometheus write request. See 2254 for more background.
Currently only write is supported. As an example, the following header would unconditionally cause globaltag=somevalue to be added to all metrics in a write request:
M3-Map-Tags-JSON: '{"tagMappers":[{"write":{"tag":"globaltag","value":"somevalue"}}]}'

#### Data Params
None.
Sample Call
curl 'http://localhost:7201/api/v1/query_range?query=abs(http_requests_total)&start=1530220860&end=1530220900&step=15s'
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "code": "200",
          "handler": "graph",
          "method": "get"
        },
        "values": [
          [
            1530220860,
            "6"
          ],
          [
            1530220875,
            "6"
          ],
          [
            1530220890,
            "6"
          ]
        ]
      },
      {
        "metric": {
          "code": "200",
          "handler": "label_values",
          "method": "get"
        },
        "values": [
          [
            1530220860,
            "6"
          ],
          [
            1530220875,
            "6"
          ],
          [
            1530220890,
            "6"
          ]
        ]
      }
    ]
  }
}



### ClusterCondition
ClusterCondition represents various conditions the cluster can be in.

Field	Description	Scheme	Required
type	Type of cluster condition.	ClusterConditionType	false
status	Status of the condition (True, False, Unknown).	corev1.ConditionStatus	false
lastUpdateTime	Last time this condition was updated.	string	false
lastTransitionTime	Last time this condition transitioned from one status to another.	string	false
reason	Reason this condition last changed.	string	false
message	Human-friendly message about this condition.	string	false
Back to TOC

### ClusterSpec
ClusterSpec defines the desired state for a M3 cluster to be converge to.

Field	Description	Scheme	Required
image	Image specifies which docker image to use with the cluster	string	false
replicationFactor	ReplicationFactor defines how many replicas	int32	false
numberOfShards	NumberOfShards defines how many shards in total	int32	false
isolationGroups	IsolationGroups specifies a map of key-value pairs. Defines which isolation groups to deploy persistent volumes for data nodes	[]IsolationGroup	false
namespaces	Namespaces specifies the namespaces this cluster will hold.	[]Namespace	false
etcdEndpoints	EtcdEndpoints defines the etcd endpoints to use for service discovery. Must be set if no custom configmap is defined. If set, etcd endpoints will be templated in to the default configmap template.	[]string	false
keepEtcdDataOnDelete	KeepEtcdDataOnDelete determines whether the operator will remove cluster metadata (placement + namespaces) in etcd when the cluster is deleted. Unless true, etcd data will be cleared when the cluster is deleted.	bool	false
enableCarbonIngester	EnableCarbonIngester enables the listener port for the carbon ingester	bool	false
configMapName	ConfigMapName specifies the ConfigMap to use for this cluster. If unset a default configmap with template variables for etcd endpoints will be used. See \"Configuring M3DB\" in the docs for more.	*string	false
podIdentityConfig	PodIdentityConfig sets the configuration for pod identity. If unset only pod name and UID will be used.	*PodIdentityConfig	false
containerResources	Resources defines memory / cpu constraints for each container in the cluster.	corev1.ResourceRequirements	false
dataDirVolumeClaimTemplate	DataDirVolumeClaimTemplate is the volume claim template for an M3DB instance's data. It claims PersistentVolumes for cluster storage, volumes are dynamically provisioned by when the StorageClass is defined.	*corev1.PersistentVolumeClaim	false
podSecurityContext	PodSecurityContext allows the user to specify an optional security context for pods.	*corev1.PodSecurityContext	false
securityContext	SecurityContext allows the user to specify a container-level security context.	*corev1.SecurityContext	false
imagePullSecrets	ImagePullSecrets will be added to every pod.	[]corev1.LocalObjectReference	false
envVars	EnvVars defines custom environment variables to be passed to M3DB containers.	[]corev1.EnvVar	false
labels	Labels sets the base labels that will be applied to resources created by the cluster. // TODO(schallert): design doc on labeling scheme.	map[string]string	false
annotations	Annotations sets the base annotations that will be applied to resources created by the cluster.	map[string]string	false
tolerations	Tolerations sets the tolerations that will be applied to all M3DB pods.	[]corev1.Toleration	false
priorityClassName	PriorityClassName sets the priority class for all M3DB pods.	string	false
nodeEndpointFormat	NodeEndpointFormat allows overriding of the endpoint used for a node in the M3DB placement. Defaults to \"{{ .PodName }}.{{ .M3DBService }}:{{ .Port }}\". Useful if access to the cluster from other namespaces is desired. See \"Node Endpoint\" docs for full variables available.	string	false
hostNetwork	HostNetwork indicates whether M3DB pods should run in the same network namespace as the node its on. This option should be used sparingly due to security concerns outlined in the linked documentation. https://kubernetes.io/docs/concepts/policy/pod-security-policy/#host-namespaces	bool	false
dnsPolicy	DNSPolicy allows the user to set the pod's DNSPolicy. This is often used in conjunction with HostNetwork.+optional	*corev1.DNSPolicy	false
externalCoordinatorSelector	Specify a \"controlling\" coordinator for the cluster It is expected that there is a separate standalone coordinator cluster It is externally managed - not managed by this operator It is expected to have a service endpoint Setup this db cluster, but do not assume a co-located coordinator Instead provide a selector here so we can point to a separate coordinator service Specify here the labels required for the selector	map[string]string	false
initContainers	Custom setup for db nodes can be done via initContainers Provide the complete spec for the initContainer here If any storage volumes are needed in the initContainer see InitVolumes below	[]corev1.Container	false
initVolumes	If the InitContainers require any storage volumes Provide the complete specification for the required Volumes here	[]corev1.Volume	false
podMetadata	PodMetadata is for any Metadata that is unique to the pods, and does not belong on any other objects, such as Prometheus scrape tags	metav1.ObjectMeta	false
Back to TOC

### IsolationGroup
IsolationGroup defines the name of zone as well attributes for the zone configuration

Field	Description	Scheme	Required
name	Name is the value that will be used in StatefulSet labels, pod labels, and M3DB placement \"isolationGroup\" fields.	string	true
nodeAffinityTerms	NodeAffinityTerms is an array of NodeAffinityTerm requirements, which are ANDed together to indicate what nodes an isolation group can be assigned to.	[]NodeAffinityTerm	false
numInstances	NumInstances defines the number of instances.	int32	true
storageClassName	StorageClassName is the name of the StorageClass to use for this isolation group. This allows ensuring that PVs will be created in the same zone as the pinned statefulset on Kubernetes < 1.12 (when topology aware volume scheduling was introduced). Only has effect if the clusters dataDirVolumeClaimTemplate is non-nil. If set, the volume claim template will have its storageClassName field overridden per-isolationgroup. If unset the storageClassName of the volumeClaimTemplate will be used.	string	false
Back to TOC

### M3DBCluster
M3DBCluster defines the cluster

Field	Description	Scheme	Required
metadata		metav1.ObjectMeta	false
type		string	true
spec		ClusterSpec	true
status		M3DBStatus	false
Back to TOC

### M3DBClusterList
M3DBClusterList represents a list of M3DB Clusters

Field	Description	Scheme	Required
metadata		metav1.ListMeta	false
items		[]M3DBCluster	true
Back to TOC

### M3DBStatus
M3DBStatus contains the current state the M3DB cluster along with a human readable message

Field	Description	Scheme	Required
state	State is a enum of green, yellow, and red denoting the health of the cluster	M3DBState	false
conditions	Various conditions about the cluster.	[]ClusterCondition	false
message	Message is a human readable message indicating why the cluster is in it's current state	string	false
observedGeneration	ObservedGeneration is the last generation of the cluster the controller observed. Kubernetes will automatically increment metadata.Generation every time the cluster spec is changed.	int64	false
Back to TOC

### NodeAffinityTerm
NodeAffinityTerm represents a node label and a set of label values, any of which can be matched to assign a pod to a node.

### Field	Description	Scheme	Required
key	Key is the label of the node.	string	true
values	Values is an array of values, any of which a node can have for a pod to be assigned to it.	[]string	true
Back to TOC

### IndexOptions
IndexOptions defines parameters for indexing.

### Field	Description	Scheme	Required
enabled	Enabled controls whether metric indexing is enabled.	bool	false
blockSize	BlockSize controls the index block size.	string	false
Back to TOC

### Namespace
Namespace defines an M3DB namespace or points to a preset M3DB namespace.

Field	Description	Scheme	Required
name	Name is the namespace name.	string	false
preset	Preset indicates preset namespace options.	string	false
options	Options points to optional custom namespace configuration.	*NamespaceOptions	false
Back to TOC

### NamespaceOptions
NamespaceOptions defines parameters for an M3DB namespace. See https://m3db.github.io/m3/operational_guide/namespace_configuration/ for more details.

Field	Description	Scheme	Required
bootstrapEnabled	BootstrapEnabled control if bootstrapping is enabled.	bool	false
flushEnabled	FlushEnabled controls whether flushing is enabled.	bool	false
writesToCommitLog	WritesToCommitLog controls whether commit log writes are enabled.	bool	false
cleanupEnabled	CleanupEnabled controls whether cleanups are enabled.	bool	false
repairEnabled	RepairEnabled controls whether repairs are enabled.	bool	false
snapshotEnabled	SnapshotEnabled controls whether snapshotting is enabled.	bool	false
retentionOptions	RetentionOptions sets the retention parameters.	RetentionOptions	false
indexOptions	IndexOptions sets the indexing parameters.	IndexOptions	false
Back to TOC

### RetentionOptions
RetentionOptions defines parameters for data retention.

Field	Description	Scheme	Required
retentionPeriod	RetentionPeriod controls how long data for the namespace is retained.	string	false
blockSize	BlockSize controls the block size for the namespace.	string	false
bufferFuture	BufferFuture controls how far in the future metrics can be written.	string	false
bufferPast	BufferPast controls how far in the past metrics can be written.	string	false
blockDataExpiry	BlockDataExpiry controls the block expiry.	bool	false
blockDataExpiryAfterNotAccessPeriod	BlockDataExpiry controls the not after access period for expiration.	string	false
Back to TOC

### PodIdentity
PodIdentity contains all the fields that may be used to identify a pod's identity in the M3DB placement. Any non-empty fields will be used to identity uniqueness of a pod for the purpose of M3DB replace operations.

Field	Description	Scheme	Required
name		string	false
uid		string	false
nodeName		string	false
nodeExternalID		string	false
nodeProviderID		string	false
Back to TOC

### PodIdentityConfig
PodIdentityConfig contains cluster-level configuration for deriving pod identity.

Field	Description	Scheme	Required
sources	Sources enumerates the sources from which to derive pod identity. Note that a pod's name will always be used. If empty, defaults to pod name and UID.	[]PodIdentitySource	true