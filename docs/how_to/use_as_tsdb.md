# Using M3DB as a generic time series database

## Overview

M3 has native integrations that make it particularly easy to use it as a metrics storage for [Prometheus](../integrations/prometheus.md) and [Graphite](../integrations/graphite.md). However, M3DB can also be used as a stand alone time series database.

## Data Model

### IDs and Tags

The data model provided by M3DB is that each cluster can contain multiple namespaces. Each [namespace can be configured and tuned independently.](../operational_guide/namespace_configuration.md)

Each namespace can also be configure with its own schema (see `"Schema Modeling" section below).

Within a namespace, each time series is uniquely identified by an ID which can be any valid string / byte array. In addition, tags can be attached to any time series which makes the time series queryable using the inverted index.

M3DB's inverted index supports term (exact match) and regular expression queries over all tag values, and individual tag queries can be arbitrarily combined using `AND`, `OR`, and `NOT` operators.

For example, imagine you were developing an application to track a fleet of vehicles. One way to structure your time series could be as follows:

| Time Series ID | "type" tag value | "city" tag value          |
|----------------|------------------|---------------------------|
| vehicle_id_1   | sedan            | san_francisco             |
| vehicle_id_2   | bicycle          | san_francisco             |
| vehicle_id_3   | scooter          | chicago_region_0          |
| vehicle_id_4   | scooter          | chicago_region_1          |

This would allow users to issue queries that answer questions like:

1. "What time series IDs exist for any vehicle type operating in San Francisco?"
2. "What time series IDs exist for scooters that are NOT operating in Chicago?"
3. "What time series IDs exist where the "city" tag matches the regular expression: `chicago_region_[0-9]+`"

TODO(rartoul): Discuss the ability to perform limited amounts of aggregation queries here as well.

TODO(rartoul): Discuss ID / tags mutability.

### Data Points

Each time series in M3DB stores data as a stream of data points in the form of `<timestamp, value>` tuples. Timestamp resolution can be as granular as individual nanoseconds.

The `value` portion of the tuple is a protobuf message that matches the configured namespace schema. This means that all time series that belong to a given namespace must contain values that match the same protobuf message schema, although this limitation may be lifted in the future.

### Schema Modeling

Every M3DB namespace can be configured with a schema where the schema is a protobuf message that each value in time series must conform to.

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

While M3DB strives to support the entire [proto3 language spec](https://developers.google.com/protocol-buffers/docs/proto3), only the following features are currently supported:

1. [Scalar values](https://developers.google.com/protocol-buffers/docs/proto3#scalar)
2. Nested messages
3. Repeated fields
4. Map fields
5. Reserved fields

The following features are currently not supported:

1. `Any` fields
2. [`Oneof` fields](https://developers.google.com/protocol-buffers/docs/proto#oneof)
3. Options of any type
4. Custom field types

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

This isn't to say that the schema above shouldn't be used, but users should take into account that any data stored in the `attributes` map will be held in memory and eventually stored on disk with no compression.

For more details on the compression scheme and its limitations, review [the documentation for M3DB's compressed protobuf encoding](https://github.com/m3db/m3/blob/master/src/dbnode/encoding/proto/docs/encoding.md).


### Getting Started

#### M3DB setup

For more advanced setups, its best to follow the guide's on how to configure an [M3DB cluster manually](./cluster_hard_way.md) or [using Kubernetes](./kubernetes.md). However, this tutorial will walk you through configuring a single node setup locally for development.

First, run the following command to pull the latest M3DB image:

```
docker pull quay.io/m3db/m3dbnode:latest
```

Next, run the following command to start the M3DB container:

```
docker run -p 7201:7201 -p 7203:7203 -p 9000:9000 -p 9001:9001 -p 9002:9002 -p 9003:9003 -p 9004:9004 -p 2379:2379 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db -v $(pwd)/src/dbnode/config/m3dbnode-local-etcd-proto.yml:/etc/m3dbnode/m3dbnode.yml -v <PATH_TO_SCHEMA_PROTOBUF_FILE>:/etc/m3dbnode/default_schema.proto quay.io/m3db/m3dbnode:latest
```

Breaking that down:

- All the `-p` flags expose the necessary ports.
- The `-v $(pwd)/m3db_data:/var/lib/m3db` section creates a durable data folder that can be reused between container restarts.
- The `-v <PATH_TO_YAML_CONFIG_FILE>:/etc/m3dbnode/m3dbnode.yml` section mounts the specified configuration file in the container. [This example file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd-proto.yml) can be used as a good starting point. It configures the database to have the protobuf feature enabled and expects one namespace with the name `default` and a protobuf message name of `VehicleLocation` for the schema. You'll need to update that portion of the config if you intend to use a different schema than the example one used throughout this document. Note that hard-coding paths to the schema should only be done for local development and testing. For production use-cases, M3DB supports storing the current schema in `etcd` so that it can be update dynamically. TODO(rartoul): Document how to do that.
- The `-v <PATH_TO_SCHEMA_PROTOBUF_FILE>:/etc/m3dbnode/default_schema.proto` section mounts the protobuf file containing the schema in the container so that M3DB can load it. You can use [this example schema](https://github.com/m3db/m3/tree/master/examples/dbnode/proto_client/schema.proto) as a starting point. Is is also the same example schema that is used by the sample Go program discussed below in the "Clients" section. Also see the bullet point above about not hard coding schema files in production.

Once the M3DB container has started, issue the following CURL statement to create the `default` namespace:

```
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "4h"
}'
```

Note that the `retentionTime` is set artificially low to conserve resources.

After a few moments, the M3DB container should finish bootstrapping. At this point it should be ready to serve write and read queries.

#### Clients

M3DB only has a Go client and this is unlikely to change in the future due to the fact that the client is "fat" and contains a lot of logic that would be difficult to port to other languages.

Users interested in interacting with M3DB directly from Go applications can review [this runnable example](https://github.com/m3db/m3/tree/master/examples/dbnode/proto_client) to get an understanding of how to interact with M3DB in Go. Note that the example above uses the same `default` namespace and `VehicleLocation` schema used throughout this document so it can be run directly against an M3DB docker container setup using the "M3DB setup" instructions above.

M3DB will eventually support other languages by exposing an `M3Coordinator` endpoint which will allow users to write/read from M3DB directly using GRPC/JSON.