---
title: "Monitoring"
menuTitle: "Monitoring"
weight: 15
chapter: true
---

M3DB exposes metrics via a Prometheus endpoint. If using the [Prometheus Operator][prometheus-operator], you can apply a
`ServiceMonitor` to have your M3DB pods automatically scraped by Prometheus:

```
kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/example/prometheus-servicemonitor.yaml
```

You can visit the "targets" page of the Prometheus UI to verify the pods are being scraped. To view these metrics using
Grafana, follow the [M3 docs][m3-grafana] to install the M3DB Grafana dashboard.

[prometheus-operator]: https://github.com/coreos/prometheus-operator
[m3-grafana]: https://docs.m3db.io/integrations/grafana/
