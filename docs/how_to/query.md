# Setting up m3query

## Introduction

m3query is used to query data that is stored in M3DB. For instance, if you are using the Prometheus remote write endpoint with [m3coordinator](../integrations/prometheus.md), you can use m3query instead of the Prometheus remote read endpoint. By doing so, you get all of the benefits of m3query's engine such as [block processing](http://m3db.github.io/m3/query_engine/architecture/blocks/). Furthermore, since m3query provides a Prometheus compatible API, you can use 3rd party graphing and alerting solutions like Grafana.

## Configuration

Before setting up m3query, make sure that you have at least [one M3DB node running](single_node.md). In order to start m3query, you need to configure a `yaml` file, that will be used to connect to M3DB. Here is a link to a [sample config](https://github.com/m3db/m3/blob/master/src/query/config/m3query-local-etcd.yml) file that is used for an embedded etcd cluster within M3DB.

### Running

You can run m3query by either building and running the binary yourself:

```
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

You will notice that in the setup linked above, M3DB has just one unaggregated namespace configured. If you want aggregated metrics, you will need to set up an aggregated namespace in M3DB **and** in the m3query configuration. It it important to note that all writes go to all namespaces so as long as you include all namespaces in your query config, you will be querying all namespaces. Aggregation is done strictly by the query service. For example if you have an aggregated namespace setup in M3DB named `metrics_10s_48h`, you can add the following to the query config:

```json
- namespace: metrics_10s_48h
  type: aggregated
  retention: 48h
  resolution: 10s
```

If you run Statsite, m3agg, or some other aggregation tier, you will want to set the `all` flag under `downsample` to `false`. Otherwise, you will be aggregating metrics that have already been aggregated.

```json
- namespace: metrics_10s_48h
  type: aggregated
  retention: 48h
  resolution: 10s
  downsample:
    all: false
```

## ID generation

The default generation scheme for IDs is unfortunately prone to collisions, but is left as is for back-compat reasons. It is suggested to set the ID generation scheme to one of either "quoted" or "prepend_meta". Quoted generation scheme yields the most human-readable IDs, whereas Prepend_Meta is better for more compact IDS, or if tags are expected to contain non-ASCII characters. To set the ID generation scheme, add the following to your coordinator configuration yaml file:

```yaml
tagOptions:
  idScheme: <name>
```

As an example of how these schemes generate tags, consider a case with 4 tags, [{t1:v1}, {t2:v2}, {t3:v3}, {t4:v4}]. The following is an example of how different schemes will generate IDs.

nil: t1=v1,t2=v2,t3=v3,t4=v4,
prepend_meta: 2,2,2,2,2,2,2,2!t1v1t2v2t3v3t4v4
quoted: {t1="v1",t2="v2",t3="v3",t4="v4"}

If there is a chance that your metric tags will contain "control" characters for the nil case, specifically `=` and `,`, It is highly recommended that a scheme is specified, as the default scheme will be prone to ID collisions.

NB: Once a scheme is selected and used, be very careful about switching it, as any incoming metrics will get an ID matching the new scheme instead, effectively doubling the metric count until any of the older metric IDs rotate out of their retention period.

## Grafana

You can also set up m3query as a [datasource in Grafana](http://docs.grafana.org/features/datasources/prometheus/). To do this, add a new datasource with a type of `Prometheus`. The URL should point to the host/port running m3query. By default, m3query runs on port `7201`.
