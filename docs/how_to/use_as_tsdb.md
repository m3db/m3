# Using M3DB as a time series database

## Overview

M3 has native integrations that make it particularly easy to use it as a metrics storage for [Prometheus](../integrations/prometheus.md) and [Graphite](../integrations/graphite.md). However, M3DB can also be used as a stand alone time series database.

## Data Model

### IDs and Tags

The data model provided by M3DB is that each cluster can contain multiple namespaces. Each [namespace can be configured and tuned independently.](../operational_guide/namespace_configuration.md)

Each namespace can also be configure with its own schema (see schema section below).

Within a namespace, each time series is uniquely identified by an ID which can be any valid string / byte array. In addition, tags can be attached to any time series which makes the time series queryable using the inverted index.

M3DB's inverted index supports term (exact match) and regular expression queries over all tag values, and individual tag queries can be combined together infiitely using AND, OR, and NOT operators.

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

Each time series in M3DB stores data as a stream of data points in the form of <timestamp, value> tuples. Timestamp resolution can be as granular as individual nanoseconds.

The `value` portion of the tuple is a protobuf message that matches the configured namespace schema. This means that all time series that belong to a given namespace must contain values that match the same protobuf message schema, although this limitation may be lifted in the future.

## Schema Modeling

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

### Compression

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

however, it will not apply any form of compression to the attributes field in this schema:

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

This isn't to say that the schema above shouldn't be used, but users should take into account that any data stored in the attributes map will be held in memory and eventually stored on disk with no compression.

For more details on the compression scheme and its limitations, review [the documentation for M3DB's compressed protobuf encoding](https://github.com/m3db/m3/blob/master/src/dbnode/encoding/proto/docs/encoding.md).
