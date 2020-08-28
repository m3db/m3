---
title: "Monitoring"
date: 2020-05-08T12:46:15-04:00
draft: true
---

Monitoring
M3DB exposes metrics via a Prometheus endpoint. If using the Prometheus Operator, you can apply a ServiceMonitor to have your M3DB pods automatically scraped by Prometheus:

kubectl apply -f https://raw.githubusercontent.com/m3db/m3db-operator/master/example/prometheus-servicemonitor.yaml
You can visit the "targets" page of the Prometheus UI to verify the pods are being scraped. To view these metrics using Grafana, follow the M3 docs to install the M3DB Grafana dashboard.