# FAQs

- **Is there a way to disable M3DB embedded `etcd` and just use an external `etcd` cluster?**
Yes, you can definitely do that. It's all just about setting the etcd endpoints in config as etcd hosts instead of M3DB hosts. 

- **Is there a client that lets me send metrics to m3coordinator without going through Prometheus?**
Yes, you can use the [Prometheus remote write client](https://github.com/m3db/prometheus_remote_client_golang/).

- **Why does my dbnode keep OOM’ing?**
Refer to the [troubleshooting guide](../troubleshooting/index.md).

- **Do you support PromQL?**
Yes, M3Query and M3Coordinator both support PromQL.

- **Does M3DB store both data and (tag) metadata on the same node?**
Yes it stores the data as well as the tags since it has an embedded index. Make sure you have `IndexEnabled` set to `true` in your namespace configuration

- **How are writes handled and how is the data kept consistent within M3DB?**
M3 uses quorum/majority consistency to ensure data is written to replicas in a way that can be read back consistently. 
For example, if you have a replication factor of 3 and your set your write and read consistencies to quorum, then all writes will only succeed if they make it to at least 2 of the 3 replicas, and reads will only succeed if they get results back from at least 2 of the 3 replicas

- **Do I need to restart M3DB if I add a namespace?**
If you’re adding namespaces, the m3dbnode process will pickup the new namespace without a restart.

- **Do I need to restart M3DB if I change or delete a namespace?**
If you’re removing or modifying an existing namespace, you’ll need to restart the m3dbnode process. It is recommended to restart one node at a time and wait for a node to be completely bootstrapped before restarting another node.

- **How do I set up aggregation in the coordinator?**
Refer to the [Aggregation section](../how_to/query) of the M3Query how-to guide.

- **How do I set up aggregation using a separate aggregation tier?**
<Fill this in>

- **Can you delete metrics from M3DB?**
Not yet, but that functionality is currently being worked on.

- **How can you tell if a node is snapshotting?**
You can check if your nodes are snapshotting by looking at the `Background tasks` tab in the [M3DB Grafana dashboard](https://grafana.com/dashboards/8126).

- **How do you list all available API endpoints?**
See [M3DB openhttps://m3db.io/openapi

- **What is the recommended way to upgrade my M3 stack?**
<Fill this in>

- **When graphing my Prometheus data in Grafana, I see gaps. How do I resolve this?**
This is due to M3 having a concept of `null` datapoints whereas Prometheus does not. To resolve this, change `Stacking & Null value` to `Connected` under the `Visualization` tab of your graph.

- **I am receiving the error `"could not create etcd watch","error":"etcd watch create timed out after 10s for key: _sd.placement/default_env/m3db"`**
This is due to the fact that M3DB, M3Coordinator, etc. could not connect to the `etcd` server. Make sure that the endpoints listed under in the following config section are correct AND the correct configuration file is being used.
```
etcdClusters:
  - zone: embedded
    endpoints:
      - HOST1_STATIC_IP_ADDRESS:2379
      - HOST2_STATIC_IP_ADDRESS:2379
      - HOST3_STATIC_IP_ADDRESS:2379
``` 

- **How can I get a heap dump, cpu profile, etc.**
See our docs on the [/debug/dump api](../troubleshooting/index.md)

- **How can I see the cardinality of my metrics?**
See the `Ticking` graph on [M3DB dashboard](https://grafana.com/dashboards/8126).

- **How much memory utilization should I run M3DB at?**
We recommend not going above 40%.

- **What is the recommended hardware to run on?**
<Fill this in>
