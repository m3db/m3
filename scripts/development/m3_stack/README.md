# Local Development

This docker-compose file will setup the following environment:

1. 3 M3DB nodes with a single node acting as an ETCD seed
2. 1 M3Coordinator node
3. 1 Grafana node (with a pre-configured Prometheus source)
4. 1 Prometheus node that scrapes the M3DB/M3Coordinator nodes and writes the metrics to M3Coordinator

## Usage

Use the `start.sh` and `stop.sh` scripts

## Grafana

Use Grafana by navigating to `http://localhost:3000` and using `admin` for both the username and password. The M3DB dashboard should already be populated and working.

## Prometheus

Use Prometheus by navigating to `http://localhost:9090`

## Increasing Load

Load can easily be increased by modifying the `prometheus.yml` file to reduce the scrape interval to `1s`

## Containers Hanging / Unresponsive

Running the entire stack can be resource intensive. If the containers are unresponsive try increasing the amount of cores and memory that the docker daemon is allowed to use.