# Grafana

M3 supports a variety of Grafana integrations.

## Prometheus / Graphite Sources

M3Coordinator can function as a datasource for Prometheus as well Graphite. See the [Prometheus integration](./prometheus.md) and [Graphite integration](./graphite.md) documents respectively for more information

## Pre-configured Prometheus Dashboards

All M3 applications expose Prometheus metrics on port `7201` by default as described in the [Prometheus integration guide](./prometheus.md), so if you're already monitoring your M3 stack with Prometheus and Grafana you can use our pre-configured dashboards.

M3DB Prometheus / Grafana dashboard: https://grafana.com/dashboards/8126

M3Coordinator Prometheus / Grafana dashboard: TODO