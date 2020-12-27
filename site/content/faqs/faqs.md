---
title: "Operational"
weight: 9
chapter: true
---

- **Is there a way to disable M3DB embedded `etcd` and just use an external `etcd` cluster?**
Yes, you can definitely do that. It's all just about setting the etcd endpoints in config as etcd hosts instead of M3DB hosts. See [these docs](/docs/operational_guide/etcd#external-etcd) for more information on configuring an external `etcd` cluster.

- **Is there a client that lets me send metrics to m3coordinator without going through Prometheus?**
Yes, you can use the [Prometheus remote write client](https://github.com/m3db/prometheus_remote_client_golang/).

- **Why does my dbnode keep OOM’ing?**
Refer to the [troubleshooting guide](/docs/faqs/troubleshooting).

- **Do you support PromQL?**
Yes, M3Query and M3Coordinator both support PromQL.

- **Do you support Graphite?**
Yes, M3Query and M3Coordinator both support Graphite.

- **Does M3DB store both data and (tag) metadata on the same node?**
Yes it stores the data (i.e. the timeseries datapoints) as well as the tags since it has an embedded index. Make sure you have `IndexEnabled` set to `true` in your namespace configuration

- **How are writes handled and how is the data kept consistent within M3DB?**
M3 uses quorum/majority consistency to ensure data is written to replicas in a way that can be read back consistently. 
For example, if you have a replication factor of 3 and you set your write and read consistencies to quorum, then all writes will only succeed if they make it to at least 2 of the 3 replicas, and reads will only succeed if they get results back from at least 2 of the 3 replicas

- **Do I need to restart M3DB if I add a namespace?**
If you’re adding namespaces, the m3dbnode process will pickup the new namespace without a restart.

- **Do I need to restart M3DB if I change or delete a namespace?**
If you’re removing or modifying an existing namespace, you’ll need to restart the m3dbnode process in order to complete the namespace deletion/modification process. It is recommended to restart one node at a time and wait for a node to be completely bootstrapped before restarting another node.

- **How do I set up aggregation in the coordinator?**
Refer to the [Aggregation section](/docs/how_to/query) of the M3Query how-to guide.

- **How do I set up aggregation using a separate aggregation tier?**
See this [WIP documentation](https://github.com/m3db/m3/pull/1741/files#diff-0a1009f86783ca8fd4499418e556c6f5).

- **Can you delete metrics from M3DB?**
Not yet, but that functionality is currently being worked on.

- **How can you tell if a node is snapshotting?**
You can check if your nodes are snapshotting by looking at the `Background tasks` tab in the [M3DB Grafana dashboard](https://grafana.com/dashboards/8126).

- **How do you list all available API endpoints?**
See [M3DB OpenAPI](https://m3db.io/openapi).

- **What is the recommended way to upgrade my M3 stack?**
See the [Upgrading M3](/docs/operational_guide/upgrading_m3) guide.

- **When graphing my Prometheus data in Grafana, I see gaps. How do I resolve this?**
This is due to M3 having a concept of `null` datapoints whereas Prometheus does not. To resolve this, change `Stacking & Null value` to `Connected` under the `Visualization` tab of your graph.

- **I am receiving the error `"could not create etcd watch","error":"etcd watch create timed out after 10s for key: _sd.placement/default_env/m3db"`**
This is due to the fact that M3DB, M3Coordinator, etc. could not connect to the `etcd` server. Make sure that the endpoints listed under in the following config section are correct AND the correct configuration file is being used.
```yaml
etcdClusters:
  - zone: embedded
    endpoints:
      - HOST1_STATIC_IP_ADDRESS:2379
      - HOST2_STATIC_IP_ADDRESS:2379
      - HOST3_STATIC_IP_ADDRESS:2379
``` 

- **How can I get a heap dump, cpu profile, etc.**
See our docs on the [/debug/dump api](/docs/faqs/troubleshooting)

- **How much memory utilization should I run M3DB at?**
We recommend not going above 50%.

- **What is the recommended hardware to run on?**
TBA

- **What is the recommended way to create a new namespace?**
Refer to the [Namespace configuration guide](/docs/operational_guide/namespace_configuration).

- **How can I see the cardinality of my metrics?**
Currently, the best way is to go to the [M3DB Node Details Dashboard](https://grafana.com/grafana/dashboards/8126) and look at the `Ticking` panel. However, this is not entirely accurate because of the way data is stored in M3DB -- time series are stored inside time-based blocks that you configure. In actuality, the `Ticking` graph shows you how many unique series there are for the most recent block that has persisted. In the future, we plan to introduce easier ways to determine the number of unique time series. 
