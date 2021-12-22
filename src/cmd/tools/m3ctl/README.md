# M3DB CLI Tool

This is a CLI tool to do some things that may be desirable for
cluster introspection, or for operational purposes.

Where configuration data is required its provided via YAML.

You can:

* create a database per the simplified database create API
* list namespaces
* delete namespaces
* list placements
* delete placements
* list topics
* delete topics
* add nodes
* remove nodes

NOTE: This tool can delete namespaces and placements.  It can be
quite hazardous if used without adequate understanding of your m3db
cluster's topology, or without a decent understanding of how m3db
works.

## Examples

```
# show help
m3ctl -h
# create a database
m3ctl apply -f ./database/examples/dbcreate.yaml
# list namespaces
m3ctl get ns
# delete a namespace
m3ctl delete ns -id default
# list service placements (m3db/m3coordinator/m3aggregator)
m3ctl get pl <service>
# list topics
m3ctl get topic --header 'Cluster-Environment-Name: namespace/m3db-cluster-name, Topic-Name: aggregator_ingest'
# point to some remote and list namespaces
m3ctl -endpoint http://localhost:7201 get ns
# check the namespaces in a kubernetes cluster
# first setup a tunnel via kubectl port-forward ... 7201
m3ctl -endpoint http://localhost:7201 get ns
# list the ids of the m3db placements
m3ctl -endpoint http://localhost:7201 get pl m3db | jq .placement.instances[].id
```

Some example yaml files for the "apply" subcommand are provided in the yaml/examples directory.
Here's one to initialize a topology:

```yaml
---
operation: init
num_shards: 64
replication_factor: 1
instances:
  - id: nodeid1
    isolation_group: isogroup1
    zone: etcd1
    weight: 100
    endpoint: node1:9000
    hostname: node1
    port: 9000
  - id: nodeid2
    isolation_group: isogroup2
    zone: etcd1
    weight: 100
    endpoint: node2:9000
    hostname: node2
    port: 9000
  - id: nodeid3
    isolation_group: isogroup3
    zone: etcd1
    weight: 100
    endpoint: node3:9000
    hostname: node3
    port: 9000
```

See the examples directories below.

# References

 * [Operational guide](https://docs.m3db.io/operational_guide)
 * [API docs](https://www.m3db.io/openapi/)
