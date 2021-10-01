# Local Development

This docker-compose file will setup the following environment:

1. 1 ETCD
2. 1 M3Coordinator node
3. 1 M3 Aggregator node
4. 2 Prometheus instances with `remote-write-receiver` feature enabled in order to accept remote writes.   
5. 1 Grafana node (with a pre-configured Prometheus source)
6. 1 Prometheus node that scrapes the M3 Components and Prometheus instances. 
   In addition, it scrapes CAdvisor /metrics endpoints exposed by using `kubectl proxy`.

The environment variables that let's you configure this setup are:
- `BUILD_M3AGGREGATOR=true`: forces build of local M3 Aggregator.
- `BUILD_M3COORDINATOR=true`: forces build of local M3 Aggregator.
- `WIPE_DATA=true`: cleans up docker state when using `stop_m3.sh`.

## Usage

Use the `start_m3.sh` and `stop_m3.sh` scripts.

If you want to scrape CAdvisor metrics from Kubernetes cluster:
- Run `port_forward_kube.sh`
- Run `start_m3.sh`

## Grafana

Use Grafana by navigating to `http://localhost:3000` and using `admin` for both the username and password. The M3DB dashboard should already be populated and working.

## Containers Hanging / Unresponsive

Running the entire stack can be resource intensive. If the containers are unresponsive try increasing the amount of cores and memory that the docker daemon is allowed to use.
