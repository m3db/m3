M3DB Tool
========

This is a CLI tool to do some things that may be desirable for cluster introspection, or for operational purposes.

Where configuration data is required its provided via YAML.

You can:

* create a database per the simplified database create API
* list namespaces
* delete namespaces
* list placements
* delete placements
* add nodes
* remove nodes

NOTE: This tool can delete namespaces and placements.  It can be quite hazardous if used without adequate understanding of your m3db cluster's topology, or without a decent understanding of how m3db works.

Examples
-------

    # show help
    m3db-tool -h
    # create a database
    m3db-tool db -create ./database/examples/devel.yaml
    # list namespaces
    m3db-tool ns
    # delete a namespace
    m3db-tool ns -delete default
    # list placements
    m3db-tool pl
    # point to some remote and list namespaces
    m3db-tool -endpoint http://localhost:7201 ns
    # list the ids of the placements
    m3db-tool -endpoint http://localhost:7201 pl | jq .placement.instances[].id

Some example yaml files are provided in the examples directories. Here's one for database creation:

    ---
    type: cluster
    namespace_name: default
    retention_time: 168h
    num_shards: 64
    replication_factor: 1
    hosts:
    - id: m3db_seed
      isolation_group: rack-a
      zone: embedded
      weight: 1024
      endpoint: m3db_seed:9000
      hostname: m3db_seed
      port: 9000
    

See the examples directories below.

References
==========

[https://m3db.github.io/m3/operational_guide](operational guide)
[api docs](https://www.m3db.io/openapi/)
