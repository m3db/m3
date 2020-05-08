---
title: "Namespace"
date: 2020-05-08T12:46:59-04:00
draft: true
---

Namespaces
M3DB uses the concept of namespaces to determine how metrics are stored and retained. The M3DB operator allows a user to define their own namespaces, or to use a set of presets we consider to be suitable for production use cases.

Namespaces are configured as part of an m3dbcluster spec.

Presets
10s:2d
This preset will store metrics at 10 second resolution for 2 days. For example, in your cluster spec:

spec:
...
  namespaces:
  - name: metrics-short-term
    preset: 10s:2d
1m:40d
This preset will store metrics at 1 minute resolution for 40 days.

spec:
...
  namespaces:
  - name: metrics-long-term
    preset: 1m:40d
Custom Namespaces
You can also define your own custom namespaces by setting the NamespaceOptions within a cluster spec. The API lists all available fields. As an example, a namespace to store 7 days of data may look like:

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


Node Affinity & Cluster Topology
Node Affinity
Kubernetes allows pods to be assigned to nodes based on various critera through node affinity.

M3DB was built with failure tolerance as a core feature. M3DB's isolation groups allow shards to be placed across failure domains such that the loss of no single domain can cause the cluster to lose quorum. More details on M3DB's resiliency can be found in the deployment docs.

By leveraging Kubernetes' node affinity and M3DB's isolation groups, the operator can guarantee that M3DB pods are distributed across failure domains. For example, in a Kubernetes cluster spread across 3 zones in a cloud region, the isolationGroups configuration below would guarantee that no single zone failure could degrade the M3DB cluster.

M3DB is unaware of the underlying zone topology: it just views the isolation groups as group1, group2, group3 in its placement. Thanks to the Kubernetes scheduler, however, these groups are actually scheduled across separate failure domains.

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
...
spec:
  replicationFactor: 3
  isolationGroups:
  - name: group1
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-b
  - name: group2
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-c
  - name: group3
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-d
Tolerations
In addition to allowing pods to be assigned to certain nodes via node affinity, Kubernetes allows pods to be repelled from nodes through taints if they don't tolerate the taint. For example, the following config would ensure:

Pods are spread across zones.

Pods are only assigned to nodes in the m3db-dedicated-pool pool.

No other pods could be assigned to those nodes (assuming they were tainted with the taint m3db-dedicated-taint).

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
...
spec:
  replicationFactor: 3
  isolationGroups:
  - name: group1
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-b
    - key: nodepool
      values:
      - m3db-dedicated-pool
  - name: group2
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-c
    - key: nodepool
      values:
      - m3db-dedicated-pool
  - name: group3
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-d
    - key: nodepool
      values:
      - m3db-dedicated-pool
  tolerations:
  - key: m3db-dedicated
    effect: NoSchedule
    operator: Exists
Example Affinity Configurations
Zonal Cluster
The examples so far have focused on multi-zone Kubernetes clusters. Some users may only have a cluster in a single zone and accept the reduced fault tolerance. The following configuration shows how to configure the operator in a zonal cluster.

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
...
spec:
  replicationFactor: 3
  isolationGroups:
  - name: group1
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-b
  - name: group2
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-b
  - name: group3
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-b
6 Zone Cluster
In the above examples we created clusters with 1 isolation group in each of 3 zones. Because values within a single NodeAffinityTerm are OR'd, we can also spread an isolationgroup across multiple zones. For example, if we had 6 zones available to us:

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
...
spec:
  replicationFactor: 3
  isolationGroups:
  - name: group1
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-a
      - us-east1-b
  - name: group2
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-c
      - us-east1-d
  - name: group3
    numInstances: 3
    nodeAffinityTerms:
    - key: failure-domain.beta.kubernetes.io/zone
      values:
      - us-east1-e
      - us-east1-f
No Affinity
If there are no failure domains available, one can have a cluster with no affinity where the pods will be scheduled however Kubernetes would place them by default:

apiVersion: operator.m3db.io/v1alpha1
kind: M3DBCluster
...
spec:
  replicationFactor: 3
  isolationGroups:
  - name: group1
    numInstances: 3
  - name: group2
    numInstances: 3
  - name: group3
    numInstances: 3

Node Endpoint
M3DB stores an endpoint field on placement instances that is used for communication between DB nodes and from other components such as the coordinator.

The operator allows customizing the format of this endpoint by setting the nodeEndpointFormat field on a cluster spec. The format of this field uses Go templates, with the following template fields currently supported:

Field	Description
PodName	Name of the pod
M3DBService	Name of the generated M3DB service
PodNamespace	Namespace the pod is in
Port	Port M3DB is serving RPCs on
The default format is:

{{ .PodName }}.{{ .M3DBService }}:{{ .Port }}
As an example of an override, to expose an M3DB cluster to containers in other Kubernetes namespaces nodeEndpointFormat can be set to:

{{ .PodName }}.{{ .M3DBService }}.{{ .PodNamespace }}:{{ .Port }}