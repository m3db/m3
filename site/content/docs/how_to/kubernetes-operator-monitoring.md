---
title: Using the Kubernetes Operator to Monitor a Cluster
weight: 3
---

M3DB exposes metrics via a Prometheus endpoint. If using the [Prometheus Operator][https://github.com/coreos/prometheus-operator], you can apply a
`ServiceMonitor` to have your M3 pods automatically scraped by Prometheus:

```shell
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/example/prometheus-servicemonitor.yaml
```

You can visit the _targets_ page of the Prometheus UI to verify it is scraping the pods. To view these metrics using
Grafana, follow the [M3 docs][https://docs.m3db.io/integrations/grafana/] to install the M3DB Grafana dashboard.