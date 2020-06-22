# M3DB on Kubernetes

This doc is aimed at developers building M3DB on Kubernetes. End users should see our
[how-to](https://docs.m3db.io/how_to/kubernetes) guide for more info.

## Bundling

In order to make it possible to set up m3db using a single YAML file that can be passed as a URL to `kubectl apply`, we
bundle our manifests into a single `bundle.yaml` file. In order to create a bundle, simply run `build_bundle.sh`. It
will take care of ordering (i.e. `Namespace` object comes first) and produce a single YAML file.

## `m3nsch`

A Kubernetes manifests for running `m3nsch` is included. `m3nsch` is a tool for load-testing M3DB clusters. The manifest
will configure `m3nsch` to generate load for a cluster created according to `bundle.yaml`. The `m3nsch` setup will
contain 2 deployments: 1 for the agent which generates and sends load and 1 for the client from which you can control
the agent. For example:

```
$ kubectl apply -f kube/m3nsch.yaml
configmap/m3nsch-server-config created
service/m3nsch-agent created
service/m3nsch-agent-debug created
deployment.apps/m3nsch-server created
deployment.apps/m3nsch-client created

$ ./kube/scripts/m3nsch_client.sh init -t foo -z default_zone -v default_env -n m3db-cluster
...
2018/09/30 15:24:16 Go Runtime version: go1.10.2
2018/09/30 15:24:16 Build Version:      v0.4.5
2018/09/30 15:24:16 Build Revision:     aa253ca01
2018/09/30 15:24:16 Build Branch:       schallert/m3nsch_update
2018/09/30 15:24:16 Build Date:         2018-09-29-21:31:30
2018/09/30 15:24:16 Build TimeUnix:     1538256690

$ ./kube/scripts/m3nsch_client.sh start
...
2018/09/30 15:24:23 Go Runtime version: go1.10.2
2018/09/30 15:24:23 Build Version:      v0.4.5
2018/09/30 15:24:23 Build Revision:     aa253ca01
2018/09/30 15:24:23 Build Branch:       schallert/m3nsch_update
2018/09/30 15:24:23 Build Date:         2018-09-29-21:31:30
2018/09/30 15:24:23 Build TimeUnix:     1538256690
15:24:23.205737[I] workload started!
```

You can view the stats of the ongoing benchmark via `m3nsch_server`'s prometheus endpoints, or using our [grafana dashboard][dash]:
```
$ kubectl port-forward svc/m3nch-agent 12580
$ curl -sSf localhost:12580/metrics | rg write.+success
# TYPE write_req_success counter
write_req_success 146000
```

[dash]: https://raw.githubusercontent.com/m3db/m3/master/integrations/grafana/m3nsch_dashboard.json
