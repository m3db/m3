# Setting up m3query

## Introduction

m3query is used to query data that is stored in M3DB. For instance, if you are using the Prometheus remote write endpoint with [m3coordinator](../integrations/prometheus.md), you can use m3query instead of the Prometheus remote read endpoint. By doing so, you get all of the benefits of m3query's engine such as [block processing](../m3query/architecture/blocks.md). Furthermore, since m3query provides a Prometheus compatible API, you can use 3rd party graphing and alerting solutions like Grafana.

## Configuration

Before setting up m3query, make sure that you have at least [one M3DB node running](single_node.md). In order to start m3query, you need to configure a `yaml` file, that will be used to connect to M3DB. Here is a link to a [sample config](https://github.com/m3db/m3/blob/master/src/query/config/m3query-local-etcd.yml) file that is used for an embedded etcd cluster within M3DB.

### Running

You can run m3query by either building and running the binary yourself:

```bash
make m3query
./bin/m3query -f ./src/query/config/m3query-local-etcd.yml
```

Or you can run it with Docker using the Docker file located at `$GOPATH/src/github.com/m3db/m3/docker/m3query/Dockerfile`.

### Namespaces

All namespaces that you wish to query from must be configured when [setting up M3DB](single_node.md). If you wish to add or change an existing namespace, please follow the namespace operational guide [here](../operational_guide/namespace_configuration.md).

### etcd

The configuration file linked above uses an embedded etcd cluster, which is fine for development purposes. However, if you wish to use this in production, you will want an [external etcd](../operational_guide/etcd.md) cluster.

<!-- TODO: link to etcd operational guide -->

## Aggregation

You will notice that in the setup linked above, M3DB has just one unaggregated namespace configured. If you want aggregated metrics, you will need to set up an aggregated namespace in M3DB **and** in the m3query configuration. It is important to note that all writes go to all namespaces so as long as you include all namespaces in your query config, you will be querying all namespaces. Aggregation is done strictly by the query service. For example if you have an aggregated namespace setup in M3DB named `metrics_10s_48h`, you can add the following to the query config:

```yaml
- namespace: metrics_10s_48h
  type: aggregated
  retention: 48h
  resolution: 10s
```

### Disabling automatic aggregation

If you run Statsite, m3agg, or some other aggregation tier, you will want to set the `all` flag under `downsample` to `false`. Otherwise, you will be aggregating metrics that have already been aggregated.

```yaml
- namespace: metrics_10s_48h
  type: aggregated
  retention: 48h
  resolution: 10s
  downsample:
    all: false
```

## ID generation

The default generation scheme for IDs, `legacy`, is unfortunately prone to collisions, but remains the default for backwards compatibility reasons. It is suggested to set the ID generation scheme to one of either `quoted` or `prepend_meta`. `quoted` generation scheme yields the most human-readable IDs, whereas `prepend_meta` is better for more compact IDs, or if tags are expected to contain non-ASCII characters. To set the ID generation scheme, add the following to your m3coordinator configuration yaml file:

```yaml
tagOptions:
  idScheme: <name>
```

As an example of how these schemes generate IDs, consider a series with the following 4 tags,
`[{"t1":v1}, {t2:"v2"}, {t3:v3}, {t4:v4}]`. The following is an example of how different schemes will generate IDs.

```
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
