---
title: Query Data with M3 Query
weight: 4
---

{{< fileinclude file="m3query_intro.md" >}}

## Prerequisites

Before running M3 Query, you need to have at least [one M3DB node running](/docs/quickstart).

## Configuring M3 Query

To start M3 Query, you need a YAML configuration file that specifies the M3 nodes it connects to.

{{% notice tip %}}
You can use [this sample configuration file](https://github.com/m3db/m3/blob/master/src/query/config/m3query-local-etcd.yml) for experimenting with a local cluster that uses an embedded etcd cluster. [Read the etcd documentation](/docs/operational_guide/etcd) for more details on running in production with an external etcd cluster.
{{% /notice %}}

## Running M3 Query

{{< tabs name="run_m3query" >}}
{{% tab name="Prebuilt Binaries" %}}

M3 Query has pre-built binaries available for Linux and macOS. [Download the latest release from GitHub](https://github.com/m3db/m3/releases/latest).

[Download the sample configuration file](https://github.com/m3db/m3/blob/master/src/query/config/m3query-local-etcd.yml) mentioned above.

```shell
./bin/m3query -f .m3query-local-etcd.yml
```

{{% /tab %}}
{{% tab name="Self-built binary" %}}

{{< fileinclude file="build_prerequisites.md" >}}

Build and run the binary:

```shell
make m3query
./bin/m3query -f ./src/query/config/m3query-local-etcd.yml
```

{{% /tab %}}
{{% tab name="Docker" %}}

```shell
docker run quay.io/m3db/m3query:v1.1.0
```

{{% /tab %}}
{{< /tabs >}}

## Aggregation

You will notice that in the setup linked above, M3DB has just one unaggregated namespace configured. If you want aggregated metrics, you will need to set up an aggregated namespace. It is important to note that all writes go to all namespaces marked as ready. Aggregation is done strictly by the query service. As an example, to configure an aggregated namespace named `metrics_10s_48h`, you can execute the following API call:

```shell
curl -X POST {{% apiendpoint %}}services/m3db/namespace -d '{
  "name": "metrics_10s_48h",
  "options": {
    "bootstrapEnabled": true,
    "flushEnabled": true,
    "writesToCommitLog": true,
    "cleanupEnabled": true,
    "snapshotEnabled": true,
    "repairEnabled": false,
    "retentionOptions": {
      "retentionPeriodDuration": "48h",
      "blockSizeDuration": "2h",
      "bufferFutureDuration": "10m",
      "bufferPastDuration": "10m",
      "blockDataExpiry": true,
      "blockDataExpiryAfterNotAccessedPeriodDuration": "5m"
    },
    "indexOptions": {
      "enabled": true,
      "blockSizeDuration": "2h"
    },
    "aggregationOptions": {
      "aggregations": [
         {
          "aggregated": true,
          "attributes": { "resolutionDuration": "10s" }
        }
      ]
    }
  }
}'
```

### Disabling automatic aggregation

If you run Statsite, m3agg, or some other aggregation tier, you will want to set the `all` flag under `downsample` to `false`. Otherwise, you will be aggregating metrics that have already been aggregated. Using the example above, `aggregationOptions` would be configured as follows

```shell
    ...
    "aggregationOptions": {
      "aggregations": [
        {
          "aggregated": true,
          "attributes": {
            "resolutionDuration": "10s",
            "downsampleOptions": { "all": false }
          }
        }
      ]
    }
    ...
```

## ID generation

As of 1.0, the default generation scheme for IDs is `quoted`. If you are using the deprecated `legacy` scheme, please contact the M3 project contributors on the open source Slack channel.

The `quoted` generation scheme yields the most human-readable IDs, whereas `prepend_meta` is better for more compact IDs, or if tags are expected to contain non-ASCII characters. To set the ID generation scheme, add the following to your m3coordinator configuration yaml file:

```yaml
tagOptions:
  idScheme: <name>
```

As an example of how these schemes generate IDs, consider a series with the following 4 tags,
`[{"t1":v1}, {t2:"v2"}, {t3:v3}, {t4:v4}]`. The following is an example of how different schemes will generate IDs.

```shell
legacy: "t1"=v1,t2="v2",t3=v3,t4=v4,
prepend_meta: 4,2,2,4,2,2,2,2!"t1"v1t2"v2"t3v3t4v4
quoted: {\"t1\"="v1",t2="\"v2\"",t3="v3",t4="v4"}
```

If there is a chance that your metric tags will contain "control" characters, specifically `,` and `=`, it is highly recommended that one of either the `quoted` or `prepend_meta` schemes are specified, as the `legacy` scheme may cause ID collisions. As a general guideline, we suggest `quoted`, as it mirrors the more familiar Prometheus style IDs.

We technically have a fourth ID generation scheme that is used for Graphite IDs, but it is exclusive to the Graphite ingestion path and is not selectable as a general scheme.

**WARNING:** Once a scheme is selected, be very careful about changing it. If changed, all incoming metrics will resolve to a new ID, effectively doubling the metric cardinality until all of the older-style metric IDs fall out of retention.

### Migration

We recently updated our ID generation scheme in m3coordinator to avoid the collision issues discussed above. To ease migration, we're temporarily enforcing that an ID generation scheme be explicitly provided in the m3coordinator configuration files.

If you have been running m3query or m3coordinator already, you may want to counterintuitively select the collision-prone `legacy` scheme, as all the IDs for all of your current metrics would have already been generated with this scheme, and choosing another will effectively double your index size. If the twofold increase in cardinality is an acceptable increase (and unfortunately, this is likely to mean doubled cardinality until your longest retention cluster rotates out), it's suggested to choose a collision-resistant scheme instead.

An example of a configuration file for a standalone m3query instance with the ID generation scheme can be found [here](https://github.com/m3db/m3/blob/master/scripts/docker-integration-tests/prometheus/m3coordinator.yml). If you're running m3query or m3coordinator embedded, these configuration options should be nested under the `coordinator:` heading, as seen [here](https://github.com/m3db/m3/blob/28fe5e1e430a651a1d66a0a3e22617b6a7f59ec6/src/dbnode/config/m3dbnode-all-config.yml#L33).

If none of these options work for you, or you would like further clarification, please stop by our [Slack](http://bit.ly/m3slack) and we'll be happy to help you.

## Grafana

You can also set up m3query as a [datasource in Grafana](http://docs.grafana.org/features/datasources/prometheus/). To do this, add a new datasource with a type of `Prometheus`. The URL should point to the host/port running m3query. By default, m3query runs on port `7201`.
