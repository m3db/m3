# Upgrading M3

## Overview

This guide explains how to upgrade M3 from one version to another (e.g. from 0.14.0 to 0.15.0) - both on Kubernetes as well as on bare metal.
This includes upgrading:

- m3dbnode
- m3coordinator (coming soon!)
- m3query (coming soon!)
- m3aggregator (coming soon!)

## m3dbnode

### Graphs to monitor

While upgrading M3DB nodes, it's important to monitor the status of bootstrapping the individual nodes. This can be monitored using the [M3DB Node Details](https://grafana.com/grafana/dashboards/8126) graph.

### Non-Kubernetes

It is very important that for each replica set, only one node gets upgraded at a time. However, multiple nodes can be upgraded across replica sets. 

1) Download new binary (linux example below).

```bash
wget "https://github.com/m3db/m3/releases/download/v0.15.0-rc.0/m3_0.15.0-rc.0_linux_amd64.tar.gz" && tar xvzf m3_0.15.0-rc.0_linux_amd64.tar.gz && rm m3_0.15.0-rc.0_linux_amd64.tar.gz
```

2) Stop and upgrade one M3DB node at a time per replica set.

```bash
# locate m3dbnode process
ps aux | grep m3dbnode

# kill and restart m3dbnode with new binary
kill -9 <m3dbnode_pid> && ./m3_0.15.0-rc.0_linux_amd64/m3dbnode -f <config-name.yml>
```

3) Confirm m3dbnode has finished bootstrapping.

```
20:10:12.911218[I] updating database namespaces [{adds [default]} {updates []} {removals []}]
20:10:13.462798[I] node tchannelthrift: listening on 0.0.0.0:9000
20:10:13.463107[I] cluster tchannelthrift: listening on 0.0.0.0:9001
20:10:13.747173[I] node httpjson: listening on 0.0.0.0:9002
20:10:13.747506[I] cluster httpjson: listening on 0.0.0.0:9003
20:10:13.747763[I] bootstrapping shards for range starting ...
...
20:10:13.757834[I] bootstrap finished [{namespace metrics} {duration 10.1261ms}]
20:10:13.758001[I] bootstrapped
20:10:14.764771[I] successfully updated topology to 3 hosts
```

4) Repeat steps 2 and 3 until all nodes have been upgraded.

### Kubernetes

