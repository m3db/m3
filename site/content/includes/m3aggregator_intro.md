M3 Aggregator is a dedicated metrics aggregator that provides stateful stream-based downsampling before storing metrics in M3DB nodes. It uses dynamic rules stored in etcd .

It uses leader election and aggregation window tracking, leveraging etcd to manage this state, to reliably emit at-least-once aggregations for downsampled metrics to long term storage. This provides cost effective and reliable downsampling & roll up of metrics. 

M3 Coordinator can also perform this role, but M3 Aggregator shards and replicates metrics, whereas M3 Coordinator is not and requires care to deploy and run in a highly available way.

Similar to M3DB, M3 Aggregator supports clustering and replication by default. This means that metrics are correctly routed to the instance(s) responsible for aggregating each metric and you can configure multiple M3 Aggregator replicas so that there are no single points of failure for aggregation.

{{% notice warning %}}
M3 Aggregator is in heavy development to make it more usable without requiring you to write your own compatible producer and consumer.
{{% /notice %}}