---
title: "M3 Aggregation for any Prometheus remote write storage"
---

### Prometheus Remote Write

As mentioned in our integrations guide, M3 Coordinator and M3 Aggregator can be configured to write to any 
[Prometheus Remote Write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write) receiver.

### Sidecar M3 Coordinator setup

In this setup we show how to run M3 Coordinator with in process M3 Aggregator as a sidecar to receive and send metrics to a Prometheus instance via remote write protocol.

{{% notice tip %}}
It is just a matter of endpoint configuration to use any other backend in place of Prometheus such as Thanos or Cortex.
{{% /notice %}}

We are going to setup:
- 1 Prometheus instance with `remote-write-receiver` enabled.
  - It will be used as a storage and query engine.
- 1 Prometheus instance scraping M3 Coordinator and Prometheus TSDB.
- 1 M3 Coordinator with in process M3 Aggregator that is aggregating and downsampling metrics.
- Finally, we are going define some aggregation and downsampling rules as an example.

For simplicity lets put all config files in one directory and export env variable:
```shell
export CONFIG_DIR="<path to your config folder>"
```

First lets run a Prometheus instance with `remote-write-receiver` enabled:

`prometheus.yml`
{{< codeinclude file="docs/includes/integrations/prometheus/prometheus.yml" language="yaml" >}}

Now run:

```shell
docker pull prom/prometheus:latest
docker run -p 9090:9090 --name prometheus \
  -v "$CONFIG_DIR/prometheus.yml:/etc/prometheus/prometheus.yml" prom/prometheus:latest \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus \
  --web.console.libraries=/usr/share/prometheus/console_libraries \
  --web.console.templates=/usr/share/prometheus/consoles \
  --enable-feature=remote-write-receiver
```

Next we configure and run M3 Coordinator:

`m3_coord_simple.yml`
{{< codeinclude file="docs/includes/integrations/prometheus/m3_coord_simple.yml" language="yaml" >}}

Run:

```shell
docker pull quay.io/m3db/m3coordinator:latest
docker run -p 7201:7201 -p 3030:3030 --name m3coordinator \
  -v "$CONFIG_DIR/m3_coord_simple.yml:/etc/m3coordinator/m3coordinator.yml" \
  quay.io/m3db/m3coordinator:latest
```

Finally, we configure and run another Prometheus instance that is scraping M3 Coordinator and Prometheus TSDB:

`prometheus-scraper.yml`
{{< codeinclude file="docs/includes/integrations/prometheus/prometheus-scraper.yml" language="yaml" >}}

Now run:

```shell
docker run --name prometheus-scraper \
  -v "$CONFIG_DIR/prometheus-scraper.yml:/etc/prometheus/prometheus.yml" prom/prometheus:latest
```

To explore metrics we can use Grafana:

`datasource.yml`
{{< codeinclude file="docs/includes/integrations/prometheus/datasource.yml" language="yaml" >}}

Now run:

```shell
docker pull grafana/grafana:latest
docker run -p 3000:3000 --name grafana \
  -v "$CONFIG_DIR/datasource.yml:/etc/grafana/provisioning/datasources/datasource.yml" grafana/grafana:latest
```

You should be able to access Grafana on `http://localhost:3000` and explore some metrics.

### Using rollup and mapping rules

So far our setup is just forwarding metrics to a single unaggregated Prometheus instance as a passthrough. This is not really useful.

Let's make use of the in process M3 Aggregator in our M3 Coordinator and add some rollup and mapping rules.

Let's change the M3 Coordinator's configuration file by adding a new endpoint configuration for aggregated metrics:

```yaml
prometheusRemoteBackend:
  endpoints:
    ...
    - name: aggregated
      address: "http://host.docker.internal:9090/api/v1/write"
      storagePolicy:
        retention: 1440h
        resolution: 5m
        downsample:
          all: false
```

**Note:** For the simplicity of this example we are adding endpoint to same Prometheus instance. For production deployments it is likely that this will be a different Prometheus instance with different characteristics for aggregated metrics.

Next we will add some mappings rules. We will make metric `coordinator_ingest_latency_count` be written at different resolution - `5m` and drop the rest.

```yaml
downsample:
  rules:
    mappingRules:
      - name: "drop metric from unaggregated endpoint"
        filter: "__name__:coordinator_ingest_latency_count"
        drop: True
      - name: "metric @ different resolution"
        filter: "__name__:coordinator_ingest_latency_count"
        aggregations: ["Last"]
        storagePolicies:
          - retention: 1440h
            resolution: 5m
```

Final M3 Coordinator configuration:

`m3_coord_rules.yml`
{{< codeinclude file="docs/includes/integrations/prometheus/m3_coord_rules.yml" language="yaml" >}}

Now restart M3 Coordinator to pick up new configuration:
```
docker run -p 7201:7201 -p 3030:3030 --name m3coordinator \
  -v "$CONFIG_DIR/m3_coord_rules.yml:/etc/m3coordinator/m3coordinator.yml" \
  quay.io/m3db/m3coordinator:latest
```

Navigate to grafana explore tab `http://localhost:3000/explore` and enter `coordinator_ingest_latency_count`. 
After a while you should see that metric emits at `5m` intervals.

### Running in production

The following sections describe deployment scenarios that can be used to run M3 Coordinator and remote M3 Aggregator in production.

#### M3 Coordinator sidecar

In this setup each metrics scraper has an M3 Coordinator as a sidecar with in process M3 Aggregator.

Every instance of scraper is living its own life and is unaware of other M3 Coordinators.

This is a pretty straightforward setup however it has limitations:
- Coupling M3 Coordinator to a scraper means we can only run as many M3 Coordinators as we have scrapers
- M3 Coordinator is likely to require more resources than an individual scraper

#### Fleet of M3 Coordinators and M3 Aggregators

With this setup we are able to scale M3 Coordinators and M3 Aggregators independently.

This requires running 4 components separately:
- Instance of M3 Coordinator Admin to administer the cluster
- Fleet of stateless M3 Coordinators
- Fleet of in-memory stateful M3 Aggregators
- [Etcd](https://etcd.io/) cluster

**Setup external Etcd cluster**

The best documentation is to follow the official [etcd docs](https://github.com/etcd-io/etcd/tree/master/Documentation).

**Run a M3 Coordinator in Admin mode**

Refer to [Running M3 Coordinator in Admin mode](/docs/how_to/m3coordinator_admin).

**Configure Remote Write Endpoints**

Add configuration to M3 Coordinators that will be accepting metrics from scrapers.

Configuration should be similar to:
```yaml
backend: prom-remote

prometheusRemoteBackend:
  endpoints:
    # This points to a Prometheus started with `--storage.tsdb.retention.time=720h`
    - name: unaggregated
      address: "http://prometheus-raw:9090/api/v1/write"
    # This points to a Prometheus started with `--storage.tsdb.retention.time=1440h`      
    - name: aggregated
      address: "http://prometheus-agg:9090/api/v1/write"
      storagePolicy:
        # Should match retention of a Prometheus instance. Coordinator uses it for routing metrics correctly.
        retention: 1440h
        # Resolution instructs M3Aggregator to downsample incoming metrics at given rate.
        # By tuning resolution we can control how much storage Prometheus needs at the cost of query accuracy as range shrinks.
        resolution: 5m
        # when ommited defaults to
        #downsample:
        # all: true
    # Another example of prometheus configured for a very long retention but with 1h resolution
    # Because of downsample: all == false metrics are downsampled based on mapping and rollup rules.     
    - name: historical
      address: "http://prometheus-hist:9090/api/v1/write"
      storagePolicy:
        retention: 8760h
        resolution: 1h
        downsample:
          all: false
```

**Configure Remote M3 Aggregator**

Refer to [Aggregate Metrics with M3 Aggregator](/docs/how_to/m3aggregator) on details how to setup M3 Coordinator with Remote M3 Aggregator.

For administrative operations when configuring topology use M3 Coordinator Admin address from previous step.

**Configure scrapers to send metrics to M3**

At this point you should have a running fleet of M3 Coordinators and M3 Aggregators.

You should configure your load balancer to route to M3 Coordinators in round-robin fashion.
