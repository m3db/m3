---
title: "Node Affinity & Cluster Topology"
menuTitle: "Node Affinity"
weight: 13
chapter: true
---

## Node Affinity

Kubernetes allows pods to be assigned to nodes based on various critera through [node affinity][k8s-node-affinity].

M3DB was built with failure tolerance as a core feature. M3DB's [isolation groups][m3db-isogroups] allow shards to be
placed across failure domains such that the loss of no single domain can cause the cluster to lose quorum. More details
on M3DB's resiliency can be found in the [deployment docs][m3db-deployment].

By leveraging Kubernetes' node affinity and M3DB's isolation groups, the operator can guarantee that M3DB pods are
distributed across failure domains. For example, in a Kubernetes cluster spread across 3 zones in a cloud region, the
`isolationGroups` configuration below would guarantee that no single zone failure could degrade the M3DB cluster.

M3DB is unaware of the underlying zone topology: it just views the isolation groups as `group1`, `group2`, `group3` in
its [placement][m3db-placement]. Thanks to the Kubernetes scheduler, however, these groups are actually scheduled across
separate failure domains.

```yaml
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
```

## Tolerations

In addition to allowing pods to be assigned to certain nodes via node affinity, Kubernetes allows pods to be _repelled_
from nodes through [taints][k8s-taints] if they don't tolerate the taint. For example, the following config would ensure:

1. Pods are spread across zones.

2. Pods are only assigned to nodes in the `m3db-dedicated-pool` pool.

3. No other pods could be assigned to those nodes (assuming they were tainted with the taint `m3db-dedicated-taint`).

```yaml
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
```

## Example Affinity Configurations

### Zonal Cluster

The examples so far have focused on multi-zone Kubernetes clusters. Some users may only have a cluster in a single zone
and accept the reduced fault tolerance. The following configuration shows how to configure the operator in a zonal
cluster.

```yaml
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
```

### 6 Zone Cluster

In the above examples we created clusters with 1 isolation group in each of 3 zones. Because `values` within a single
[NodeAffinityTerm][node-affinity-term] are OR'd, we can also spread an isolationgroup across multiple zones. For
example, if we had 6 zones available to us:

```yaml
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
```

### No Affinity

If there are no failure domains available, one can have a cluster with no affinity where the pods will be scheduled however Kubernetes would place them by default:

```yaml
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
```

[k8s-node-affinity]: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity
[k8s-taints]: https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/
[m3db-deployment]: https://docs.m3db.io/operational_guide/replication_and_deployment_in_zones/
[m3db-isogroups]: https://docs.m3db.io/operational_guide/placement_configuration/#isolation-group
[m3db-placement]: https://docs.m3db.io/operational_guide/placement/
[node-affinity-term]: /docs/operator/api/#nodeaffinityterm
