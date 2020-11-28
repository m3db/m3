---
title: "Pod Identity"
menuTitle: "Pod Identity"
weight: 11
chapter: true
---

## Motivation

M3DB assumes that if a process is started and owns sealed shards marked as `Available` that its data for those shards is
valid and does not have to be fetched from peers. Consequentially this means it will begin serving reads for that data.
For more background on M3DB topology, see the [M3DB topology docs][topology-docs].

In most environments in which M3DB has been deployed in production, it has been on a set of hosts predetermined by
whomever is managing the cluster. This means that an M3DB instance is identified in a toplogy by its hostname, and that
when an M3DB process comes up and finds its hostname in the cluster with `Available` shards that it can serve reads for
those shards.

This does not work on Kubernetes, particularly when working with StatefulSets, as a pod may be rescheduled on a new node
or with new storage attached but its name may stay the same. If we were to naively use an instance's hostname (pod
name), and it were to get rescheduled on a new node with no data, it could assume that absence of data is valid and
begin returning empty results for read requests.

To account for this, the M3DB Operator determines an M3DB instance's identity in the topology based on a configurable
set of metadata about the pod.

## Configuration

The M3DB operator uses a configurable set of metadata about a pod to determine its identity in the M3DB placement. This
is encapsulated in the [PodIdentityConfig][pod-id-api] field of a cluster's spec. In addition to the configures sources,
a pod's name will always be included.

Every pod in an M3DB cluster is annotated with its identity and is passed to the M3DB instance via a downward API
volume.

### Sources

This section will be filled out as a number of pending PRs land.

## Recommendations

### No Persistent Storage

If not using PVs, you should set `sources` to `PodUID`:
```
podIdentityConfig:
  sources:
  - PodUID
```

This way whenever a container is rescheduled, the operator will initiate a replace and it will stream data from its
peers before serving reads. Note that not having persistent storage is not a recommended way to run M3DB.

### Remote Persistent Storage

If using remote storage you do not need to set sources, as it will default to just the pods name. The data for an M3DB
instance will move around with its container.

### Local Persistent Storage

If using persistent local volumes, you should set sources to `NodeName`. In this configuration M3DB will consider a pod
to be the same so long as it's on the same node. Replaces will only be triggered if a pod with the same name is moved to
a new host.

Note that if using local SSDs on GKE, node names may stay the same even though a VM has been recreated. We also support
`ProviderID`, which will use the underlying VM's unique ID number in GCE to identity host uniqueness.

[pod-id-api]: /docs/operator/api/#podidentityconfig
[topology-docs]: https://docs.m3db.io/operational_guide/placement/
