---
title: "Grafana"
weight: 3
---


M3 supports a variety of Grafana integrations.

## Prometheus / Graphite Sources

M3Coordinator can function as a datasource for Prometheus as well as Graphite. See the [Prometheus integration](/v0.15.17/docs/integrations/prometheus) and [Graphite integration](/v0.15.17/docs/integrations/graphite) documents respectively for more information.

## Pre-configured Prometheus Dashboards

All M3 applications expose Prometheus metrics on port `7203` by default as described in the [Prometheus integration guide](/v0.15.17/docs/integrations/prometheus), so if you're already monitoring your M3 stack with Prometheus and Grafana you can use our pre-configured dashboards.

[M3DB Prometheus / Grafana dashboard](https://grafana.com/dashboards/8126)

M3Coordinator Prometheus / Grafana dashboard: TODO

Alternatively, you can obtain the JSON for our most up-to-date dashboards from our [git repo](https://github.com/m3db/m3/blob/master/integrations/grafana) directly.