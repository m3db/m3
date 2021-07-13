---
title: "Grafana"
weight: 3
---

## Overview 

Grafana is an open-source visualization tool used to dashboarding, querying, and monitoring databases, especially used for time series databases. See more [here](https://grafana.com/). 

M3 supports Grafana integrations for both Prometheus and Graphite/StatsD metrics. It is recommeneded to use Prometheus metrics when integrating with M3DB. 

## Getting started with Grafana 

Refer to Grafana's [Getting Started documention](https://grafana.com/docs/grafana/latest/getting-started/). 

### Provisioning Grafana

Learn how to provision your Grafana instances [here](https://grafana.com/docs/grafana/latest/administration/provisioning/#datasources). 

### Datasources

Grafana suports many different datasources. When using Grafana, you will be able to switch views across your datasources. Grafana includes built-in support for [Prometheus](https://grafana.com/docs/grafana/latest/datasources/prometheus/) and [Graphite](https://grafana.com/docs/grafana/latest/datasources/graphite/). 

See a list of all supported datasources [here](https://grafana.com/docs/grafana/latest/datasources/).

### Dashboarding 

Best practices for [creating dashboards](https://grafana.com/docs/grafana/latest/best-practices/best-practices-for-creating-dashboards/). 

Best practices for [managing dashboards](https://grafana.com/docs/grafana/latest/best-practices/best-practices-for-managing-dashboards/). 

## Grafana with Prometheus Metrics

See the [Prometheus integration](/docs/integrations/prometheus) for more information on querying for Prometheus metrics using Grafana. 

### Sending metrics to M3DB

**Prometheus Remote Read and Write**: In order to send metrics to M3DB, you must enable Remote Storage by configureing your Prometheus datasources with [Remote Read](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read) and [Remote Write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write) HTTP APIs. See more about remote endpoints and storage [here](https://prometheus.io/docs/operating/integrations/#remote-endpoints-and-storage). 

See the [Prometheus integration](https://m3db.io/docs/integrations/prometheus/) documentation for more information and examples of the configurations.

### Adding Prometheus as a datasource 

Grafana includes built-in support for [Prometheus](https://grafana.com/docs/grafana/latest/datasources/prometheus/).

### Pre-configured Prometheus Dashboards

All M3 applications expose Prometheus metrics on port `7203` by default as described in the [Prometheus integration guide](/docs/integrations/prometheus), so if you're already monitoring your M3 stack with Prometheus and Grafana you can use our pre-configured dashboards.

[M3DB Prometheus / Grafana dashboard](https://grafana.com/dashboards/8126)

Alternatively, you can obtain the JSON for our most up-to-date dashboards from our [git repo](https://github.com/m3db/m3/blob/master/integrations/grafana) directly.

## Grafana with StatsD / Graphite Metrics 

See [Graphite integration](/docs/integrations/graphite) documents for more information on querying for Graphite metrics using Grafana. 

### Adding Graphite as a datasource

Grafana includes built-in support for [Graphite](https://grafana.com/docs/grafana/latest/datasources/graphite/). 

**Note**: You’ll need to set the URL to: http://<M3_COORDINATOR_HOST_NAME>:7201/api/v1/graphite. 