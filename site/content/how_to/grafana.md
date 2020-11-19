---
title: "Integrating with Grafana"
date: 2020-04-21T20:53:35-04:00
draft: true
---

### Grafana
You can also set up m3query as a datasource in Grafana. To do this, add a new datasource with a type of Prometheus. The URL should point to the host/port running m3query. By default, m3query runs on port 7201.

#### Querying With Grafana
When using the Prometheus integration with Grafana, there are two different ways you can query for your metrics. The first option is to configure Grafana to query Prometheus directly by following these instructions.
Alternatively, you can configure Grafana to read metrics directly from M3Coordinator in which case you will bypass Prometheus entirely and use M3's PromQL engine instead. To set this up, follow the same instructions from the previous step, but set the url to: http://<M3_COORDINATOR_HOST_NAME>:7201.

### Querying
M3 supports the the majority of graphite query functions and can be used to query metrics that were ingested via the ingestion pathway described above.

### Grafana
M3Coordinator implements the Graphite source interface, so you can add it as a graphite source in Grafana by following these instructions.
Note that you'll need to set the URL to: http://<M3_COORDINATOR_HOST_NAME>:7201/api/v1/graphite
Direct
You can query for metrics directly by issuing HTTP GET requests directly against the M3Coordinator /api/v1/graphite/render endpoint which runs on port 7201 by default. For example:
(export now=$(date +%s) && curl "localhost:7201/api/v1/graphite/render?target=transformNull(foo.*.baz)&from=$(($now-300))" | jq .)

will query for all metrics matching the foo.*.baz pattern, applying the transformNull function, and returning all datapoints for the last 5 minutes.

### Grafana
M3 supports a variety of Grafana integrations.

#### Prometheus / Graphite Sources
M3Coordinator can function as a datasource for Prometheus as well as Graphite. See the Prometheus integration and Graphite integration documents respectively for more information.
#### Pre-configured Prometheus Dashboards
All M3 applications expose Prometheus metrics on port 7203 by default as described in the Prometheus integration guide, so if you're already monitoring your M3 stack with Prometheus and Grafana you can use our pre-configured dashboards.
M3DB Prometheus / Grafana dashboard
M3Coordinator Prometheus / Grafana dashboard: TODO
Alternatively, you can obtain the JSON for our most up-to-date dashboards from our git repo directly.
