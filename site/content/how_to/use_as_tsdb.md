---
weight: 6
title: Using M3DB as a general purpose time series database
---


## Overview

M3 has native integrations that make it particularly easy to use it as a metrics storage for [Prometheus](/docs/integrations/prometheus) and [Graphite](/docs/integrations/graphite). M3DB can also be used as a general purpose distributed time series database by itself.

## Data Model

### IDs and Tags

M3DB's data model allows multiple namespaces, each of which can be [configured and tuned independently](/docs/operational_guide/namespace_configuration).

Each namespace can also be configured with its own schema (see "Schema Modeling" section below).

Within a namespace, each time series is uniquely identified by an ID which can be any valid string / byte array. In addition, tags can be attached to any series which makes the series queryable using the inverted index.

M3DB's inverted index supports term (exact match) and regular expression queries over all tag values, and individual tag queries can be arbitrarily combined using `AND`, `OR`, and `NOT` operators.

For example, imagine an application that tracks a fleet of vehicles. One potential structure for the time series could be as follows:

|                     | Timeseries 1  | Timeseries 2  | Timeseries 3 | Timeseries 4 |
| ------------------- | ------------- | ------------- | ------------ | ------------ |
| Timeseries ID       | vehicle_id_1  | vehicle_id_2  | vehicle_id_3 | vehicle_id_4 |
| "type" tag value    | sedan         | bike          | scooter      | scooter      |
| "city" tag value    | san_francisco | san_francisco | new_york     | chicago      |
| "version" tag value | 0_1_0         | 0_1_0         | 0_1_1        | 0_1_2        |

This would allow users to issue queries that answer questions like:

1.  "What time series IDs exist for any vehicle type operating in San Francisco?"
2.  "What time series IDs exist for scooters that are NOT operating in Chicago?"
3.  "What time series IDs exist where the "version" tag matches the regular expression: `0_1_[12]`"

TODO(rartoul): Discuss the ability to perform limited amounts of aggregation queries here as well.

TODO(rartoul): Discuss ID / tags mutability.

### Datapoints

Each time series in M3DB stores data as a stream of datapoints in the form of `<timestamp, value>` tuples. Timestamp resolution can be as granular as individual nanoseconds.

The `value` portion of the tuple is a Protobuf message that matches the configured namespace schema, which requires that all values in the current time series must also match this schema. This limitation may be lifted in the future.

### Schema Modeling

Every M3DB namespace can be configured with a Protobuf-defined schema that every value in the time series must conform to

For example, continuing with the vehicle fleet tracking example introduced earlier, a schema might look as follows:

```protobuf
syntax = "proto3";

message VehicleLocation {
  double latitude = 1;
  double longitude = 2;
  double fuel_percent = 3;
  string status = 4;
}
```

While M3DB strives to support the entire [proto3 language spec](https://developers.google.com/protocol-buffers/docs/proto3), only [the following features are currently supported](https://github.com/m3db/m3/blob/master/src/dbnode/encoding/proto/docs/encoding.md):

1.  [Scalar values](https://developers.google.com/protocol-buffers/docs/proto3#scalar)
2.  Nested messages
3.  Repeated fields
4.  Map fields
5.  Reserved fields

The following features are currently not supported:

1.  `Any` fields
2.  [`Oneof` fields](https://developers.google.com/protocol-buffers/docs/proto#oneof)
3.  Options of any type
4.  Custom field types

#### Compression

While M3DB supports schemas that contain nested messages, repeated fields, and map fields, currently it can only effectively compress top level scalar fields. For example, M3DB can compress every field in the following schema:

```protobuf
syntax = "proto3";

message VehicleLocation {
  double latitude = 1;
  double longitude = 2;
  double fuel_percent = 3;
  string status = 4;
}
```

however, it will not apply any form of compression to the `attributes` field in this schema:

```protobuf
syntax = "proto3";

message VehicleLocation {
  double latitude = 1;
  double longitude = 2;
  double fuel_percent = 3;
  string status = 4;
  map<string, string> attributes = 5;
}
```

While the latter schema is valid, the attributes field will not be compressed; users should weigh the tradeoffs between more expressive schema and better compression for each use case.

For more details on the compression scheme and its limitations, review [the documentation for M3DB's compressed Protobuf encoding](https://github.com/m3db/m3/blob/master/src/dbnode/encoding/proto/docs/encoding.md).

### Getting Started

#### M3DB setup

For more advanced setups, it's best to follow the guides on how to configure an M3DB cluster [manually](/docs/cluster/binaries_cluster) or [using Kubernetes](/docs/cluster/kubernetes_cluster). However, this tutorial will walk you through configuring a single node setup locally for development.

First, run the following command to pull the latest M3DB image:

    docker pull quay.io/m3db/m3dbnode:latest

Next, run the following command to start the M3DB container:

    docker run -p 7201:7201 -p 7203:7203 -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 -p 2379:2379 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db -v $(pwd)/src/dbnode/config/m3dbnode-local-etcd-proto.yml:/etc/m3dbnode/m3dbnode.yml -v <PATH_TO_SCHEMA_Protobuf_FILE>:/etc/m3dbnode/default_schema.proto quay.io/m3db/m3dbnode:latest

Breaking that down:

-   All the `-p` flags expose the necessary ports.
-   The `-v $(pwd)/m3db_data:/var/lib/m3db` section creates a bind mount that enables M3DB to persist data between container restarts.
-   The `-v <PATH_TO_YAML_CONFIG_FILE>:/etc/m3dbnode/m3dbnode.yml` section mounts the specified configuration file in the container which allows configuration changes by restarting the container (rather than rebuilding it). [This example file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd-proto.yml) can be used as a good starting point. It configures the database to have the Protobuf feature enabled and expects one namespace with the name `default` and a Protobuf message name of `VehicleLocation` for the schema. You'll need to update that portion of the config if you intend to use a different schema than the example one used throughout this document. Note that hard-coding paths to the schema should only be done for local development and testing. For production use-cases, M3DB supports storing the current schema in `etcd` so that it can be update dynamically. TODO(rartoul): Document how to do that as well as what kind of schema changes are safe / backwards compatible.
-   The `-v <PATH_TO_SCHEMA_Protobuf_FILE>:/etc/m3dbnode/default_schema.proto` section mounts the Protobuf file containing the schema in the container, similar to the configuration file this allows the schema to be changed by restarting the container instead of rebuilding it. You can use [this example schema](https://github.com/m3db/m3/tree/master/examples/dbnode/proto_client/schema.proto) as a starting point. Is is also the same example schema that is used by the sample Go program discussed below in the "Clients" section. Also see the bullet point above about not hard coding schema files in production.

Once the M3DB container has started, issue the following CURL statement to create the `default` namespace:

```shell
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "4h"
}'
```

Note that the `retentionTime` is set artificially low to conserve resources.

Once a namespace has finished bootstrapping, you must mark it as ready before receiving traffic by using the _{{% apiendpoint %}}namespace/ready_.

{{< tabs name="ready_namespaces" >}}
{{% tab name="Command" %}}

{{< codeinclude file="docs/includes/quickstart/ready-namespace.sh" language="shell" >}}

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "ready": true
}
```

{{% /tab %}}
{{< /tabs >}}


At this point it should be ready to serve write and read queries.

#### Clients

Note: M3DB only has a Go client; this is unlikely to change in the future due to the fact that the client is "fat" and contains a substantial amount of logic that would be difficult to port to other languages.

Users interested in interacting with M3DB directly from Go applications can reference [this runnable example](https://github.com/m3db/m3/tree/master/examples/dbnode/proto_client) to get an understanding of how to interact with M3DB in Go. Note that the example above uses the same `default` namespace and `VehicleLocation` schema used throughout this document so it can be run directly against an M3DB docker container setup using the "M3DB setup" instructions above.

M3DB will eventually support other languages by exposing an `M3Coordinator` endpoint which will allow users to write/read from M3DB directly using GRPC/JSON.
