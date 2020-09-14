---
title: "Upgrading M3"
date: 2020-04-21T20:50:39-04:00
draft: true
---

### Overview
This guide explains how to upgrade M3 from one version to another (e.g. from 0.14.0 to 0.15.0). This includes upgrading:
m3dbnode
m3coordinator
m3query
m3aggregator
m3dbnode

#### Graphs to monitor
While upgrading M3DB nodes, it's important to monitor the status of bootstrapping the individual nodes. This can be monitored using the M3DB Node Details graph. Typically, the Bootstrapped graph under Background Tasks and the graphs within the CPU and Memory Utilization give a good understanding of how well bootstrapping is going.

#### Non-Kubernetes
It is very important that for each replica set, only one node gets upgraded at a time. However, multiple nodes can be upgraded across replica sets.

1) Download new binary (linux example below).
wget "https://github.com/m3db/m3/releases/download/v$VERSION/m3_$VERSION_linux_amd64.tar.gz" && tar xvzf m3_$VERSION_linux_amd64.tar.gz && rm m3_$VERSION_linux_amd64.tar.gz

2) Stop and upgrade one M3DB node at a time per replica set using the systemd unit.
# stop m3dbnode
sudo systemctl stop m3dbnode

# start m3dbnode with the new binary (which should be placed in the path specified in the systemd unit)
sudo systemctl start m3dbnode

Note: If unable to stop m3dbnode using systemctl, use pkill instead.
# stop m3dbnode
pkill m3dbnode

# start m3dbnode with new binary
./m3_$VERSION_linux_amd64/m3dbnode -f <config-name.yml>

3) Confirm m3dbnode has finished bootstrapping.
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

4) Repeat steps 2 and 3 until all nodes have been upgraded.
Kubernetes
If running M3DB on Kubernetes, upgrade by completing the following steps.
Identify the version of m3dbnode to upgrade to on Quay.
Replace the Docker image in the StatefulSet manifest (or m3db-operator manifest) to be the new version of m3dbnode.
spec:
  image: quay.io/m3db/m3dbnode:$VERSION

Once updated, apply the updated manifest and a rolling restart will be performed.
kubectl apply -f <m3dbnode_manifest>

### Downgrading
The upgrading steps above can also be used to downgrade M3DB. However, it is important to refer to the release notes to make sure that versions are backwards compatible.
m3coordinator
m3coordinator can be upgraded using similar steps as m3dbnode, however, the images can be found here instead.
m3query
m3query can be upgraded using similar steps as m3dbnode, however, the images can be found here instead.
m3aggregator
m3aggregator can be upgraded using similar steps as m3dbnode, however, the images can be found here instead.



