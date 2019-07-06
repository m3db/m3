# kube-prometheus

This is a set of manifests slightly modified from the [kube-prometheus](https://github.com/coreos/kube-prometheus) repository.

Major modifications are:
- Removal of Alertmanager manifests (don't need Alertmanager for test environments)
- Change `prometheus-prometheus.yaml` Prometheus CRD to only use a single replica (don't need HA for test environments).
