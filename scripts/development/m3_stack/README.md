# Local Development

This docker-compose file will setup the following environment:

1. 1 M3DB nodes with a single node acting as an ETCD seed
2. 1 M3Coordinator node
3. 1 Grafana node (with a pre-configured Prometheus source)
4. 1 Prometheus node that scrapes the M3DB/M3Coordinator nodes and writes the metrics to M3Coordinator

The environment variables that let's you configure this setup are:
- `USE_MULTI_DB_NODES=true`: uses 3 database nodes instead of 1 for cluster.
- `USE_JAEGER=true`: look at traces emitted by M3 services.
- `USE_PROMETHEUS_HA=true`: send data to M3 from two HA Prometheus instances to replicate deployments of HA Prometheus sending data to M3.
- `USE_AGGREGATOR=true`: use dedicate aggregator to aggregate metrics.
- `USE_AGGREGATOR_HA=true`: use two dedicated aggregators for HA aggregated metrics.
- `USE_MULTIPROCESS_COORDINATOR=true`: use multi-process coordinator, with default number of processes configured.

## Usage

Use the `start_m3.sh` and `stop_m3.sh` scripts. Requires successful run of `make m3dbnode` from project root first.

## Grafana

Use Grafana by navigating to `http://localhost:3000` and using `admin` for both the username and password. The M3DB dashboard should already be populated and working.

## Jaeger

To start Jaeger, you need to set the environment variable `USE_JAEGER` to `true` when you run `start_m3.sh`.

```
USE_JAEGER=true ./start_m3.sh
```

To modify the sampling rate, etc. you can modify the following in your `m3dbnode.yml` file under `db`:

```yaml
tracing:
  backend: jaeger
  jaeger:
    reporter:
      localAgentHostPort: jaeger:6831
    sampler:
      type: const
      param: 1
```

Use Jaeger by navigating to `http://localhost:16686`.

## Prometheus

Use Prometheus by navigating to `http://localhost:9090`.

## Increasing Load

Load can easily be increased by modifying the `prometheus.yml` file to reduce the scrape interval to `1s`.

## Containers Hanging / Unresponsive

Running the entire stack can be resource intensive. If the containers are unresponsive, try increasing the amount of cores and memory that the docker daemon is allowed to use.
