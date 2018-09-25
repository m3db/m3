# Local Development

This docker-compose file will setup the following environment:

1. 3 M3DB nodes with a single node acting as an EtcD seed
2. 1 M3Coordinator node
3. 1 Grafana node (with a pre-configured Prometheus source)
4. 1 Prometheus node that scrapes the M3DB/M3Coordinator nodes and writes the metrics to M3Coordinator

## Usage

Use the `start.sh` and `stop.sh` scripts