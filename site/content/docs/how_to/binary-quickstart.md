---
linktitle: "Quickstart using binaries"
weight: 2
---

<!-- TODO: Combine with other quickstart? -->

# Creating a Single Node M3DB Cluster with Binaries

This guide shows how to install and configure M3DB, create a single-node cluster, and read and write metrics to it.

{{% notice warning %}}
Deploying a single-node M3DB cluster is a great way to experiment with M3DB and get an idea of what it has to offer, but is not designed for production use. To run M3DB in clustered mode, with a separate M3Coordinator [read the clustered mode guide](cluster_hard_way.md).
{{% /notice %}}

## Prebuilt binaries

M3 has pre-built binaries available for Linux and macOS. [Download the latest release from GitHub](https://github.com/m3db/m3/releases/latest).

## Build from source

### Prerequisites

-   [Go 1.10 or higher](https://golang.org/dl/)
-   [Make](https://www.gnu.org/software/make/)

### Build source

```shell
make m3dbnode
```

<!-- TODO: Separate out into included partials -->

## Start Binary

By default the binary configures a single M3DB instance containing:

-   An M3DB storage instance for time series storage. It includes an embedded tag-based metrics index and an etcd server for storing the cluster topology and runtime configuration.
-   A coordinator instance for writing and querying tagged metrics, as well as managing cluster topology and runtime configuration.

It exposes three ports:

-   `7201` to manage the cluster topology, you make most API calls to this endpoint
-   `7203` for Prometheus to scrape the metrics produced by M3DB and M3Coordinator

The command below starts the node using the specified configuration file.

{{< tabs name="start_container" >}}
{{% tab name="Pre-built binary" %}}

[Download the example configuration file](https://github.com/m3db/m3/raw/master/src/dbnode/config/m3dbnode-local-etcd.yml).

```shell
./m3dbnode -f /{FILE_LOCATION}/m3dbnode-local-etcd.yml
```

{{% notice info %}}
Depending on your operating system setup, you might need to prefix the command with `sudo`.
{{% /notice %}}

{{% /tab %}}
{{% tab name="Self-built binary" %}}

```shell
./bin/m3dbnode -f ./src/dbnode/config/m3dbnode-local-etcd.yml
```

{{% notice info %}}
Depending on your operating system setup, you might need to prefix the command with `sudo`.
{{% /notice %}}

{{% /tab %}}
{{% tab name="Output" %}}

<!-- TODO: Perfect image, pref with terminalizer -->

<!-- TODO: Update image -->

![Docker pull and run](/docker-install.gif)

{{% /tab %}}
{{< /tabs >}}

{{% notice info %}}
When running the command above on macOS you may see errors about "too many open files." To fix this in your current terminal, use `ulimit` to increase the upper limit, for example `ulimit -n 10240`.
{{% /notice %}}

## Configuration

This example uses this [sample configuration file](https://github.com/m3db/m3/raw/master/src/dbnode/config/m3dbnode-local-etcd.yml) by default.

The file groups configuration into `coordinator` or `db` sections that represent the `M3Coordinator` and `M3DB` instances of single-node cluster.

{{% notice tip %}}
You can find more information on configuring M3DB in the [operational guides section](/operational_guide/).
{{% /notice %}}

## Organizing Data with Placements and Namespaces

A time series database (TSDBs) typically consist of one node (or instance) to store metrics data. This setup is simple to use but has issues with scalability over time as the quantity of metrics data written and read increases.

As a distributed TSDB, M3DB helps solves this problem by spreading metrics data, and demand for that data, across multiple nodes in a cluster. M3DB does this by splitting data into segments that match certain criteria (such as above a certain value) across nodes into {{< glossary_tooltip text="shards" term_id="shard" >}}.

<!-- TODO: Find an image -->

If you've worked with a distributed database before, then these concepts are probably familiar to you, but M3DB uses different terminology to represent some concepts.

-   Every cluster has **one** {{< glossary_tooltip text="placement" term_id="placement" >}} that maps shards to nodes in the cluster.
-   A cluster can have **0 or more** {{< glossary_tooltip text="namespaces" term_id="namespace" >}} that are similar conceptually to tables in other databases, and each node serves every namespace for the shards it owns.

<!-- TODO: Image -->

For example, if the cluster placement states that node A owns shards 1, 2, and 3, then node A owns shards 1, 2, 3 for all configured namespaces in the cluster. Each namespace has its own configuration options, including a name and retention time for the data.

## Create a Placement and Namespace

This quickstart uses the _{{% apiendpoint %}}database/create_ endpoint that creates a namespace, and the placement if it doesn't already exist based on the `type` argument.

You can create [placements](/operational_guide/placement_configuration/) and [namespaces](/operational_guide/namespace_configuration/#advanced-hard-way) separately if you need more control over their settings.

The `namespaceName` argument must match the namespace in the `local` section of the `M3Coordinator` YAML configuration. If you [add any namespaces](/operational_guide/namespace_configuration.md) you also need to add them to the `local` section of `M3Coordinator`'s YAML config.

{{< tabs name="create_placement_namespace" >}}
{{% tab name="Command" %}}

```json
curl -X POST {{% apiendpoint %}}database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "12h"
}'
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "namespace": {
    "registry": {
      "namespaces": {
        "default": {
          "bootstrapEnabled": true,
          "flushEnabled": true,
          "writesToCommitLog": true,
          "cleanupEnabled": true,
          "repairEnabled": false,
          "retentionOptions": {
            "retentionPeriodNanos": "43200000000000",
            "blockSizeNanos": "1800000000000",
            "bufferFutureNanos": "120000000000",
            "bufferPastNanos": "600000000000",
            "blockDataExpiry": true,
            "blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
            "futureRetentionPeriodNanos": "0"
          },
          "snapshotEnabled": true,
          "indexOptions": {
            "enabled": true,
            "blockSizeNanos": "1800000000000"
          },
          "schemaOptions": null,
          "coldWritesEnabled": false,
          "runtimeOptions": null
        }
      }
    }
  },
  "placement": {
    "placement": {
      "instances": {
        "m3db_local": {
          "id": "m3db_local",
          "isolationGroup": "local",
          "zone": "embedded",
          "weight": 1,
          "endpoint": "127.0.0.1:9000",
          "shards": [
            {
              "id": 0,
              "state": "INITIALIZING",
              "sourceId": "",
              "cutoverNanos": "0",
              "cutoffNanos": "0"
            },
            …
            {
              "id": 63,
              "state": "INITIALIZING",
              "sourceId": "",
              "cutoverNanos": "0",
              "cutoffNanos": "0"
            }
          ],
          "shardSetId": 0,
          "hostname": "localhost",
          "port": 9000,
          "metadata": {
            "debugPort": 0
          }
        }
      },
      "replicaFactor": 1,
      "numShards": 64,
      "isSharded": true,
      "cutoverTime": "0",
      "isMirrored": false,
      "maxShardSetId": 0
    },
    "version": 0
  }
}
```

{{< /tab >}}
{{< /tabs >}}

Placement initialization can take a minute or two. Once all the shards have the `AVAILABLE` state, the node has finished bootstrapping, and you should see the following messages in the node console output.

<!-- TODO: Fix these timestamps -->

```shell
{"level":"info","ts":1598367624.0117292,"msg":"bootstrap marking all shards as bootstrapped","namespace":"default","namespace":"default","numShards":64}
{"level":"info","ts":1598367624.0301404,"msg":"bootstrap index with bootstrapped index segments","namespace":"default","numIndexBlocks":0}
{"level":"info","ts":1598367624.0301914,"msg":"bootstrap success","numShards":64,"bootstrapDuration":0.049208827}
{"level":"info","ts":1598367624.03023,"msg":"bootstrapped"}
```

You can check on the status by calling the _{{% apiendpoint %}}placement_ endpoint:

{{< tabs name="check_placement" >}}
{{% tab name="Command" %}}

```shell
curl {{% apiendpoint %}}placement | jq .
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "placement": {
    "instances": {
      "m3db_local": {
        "id": "m3db_local",
        "isolationGroup": "local",
        "zone": "embedded",
        "weight": 1,
        "endpoint": "127.0.0.1:9000",
        "shards": [
          {
            "id": 0,
            "state": "AVAILABLE",
            "sourceId": "",
            "cutoverNanos": "0",
            "cutoffNanos": "0"
          },
          …
          {
            "id": 63,
            "state": "AVAILABLE",
            "sourceId": "",
            "cutoverNanos": "0",
            "cutoffNanos": "0"
          }
        ],
        "shardSetId": 0,
        "hostname": "localhost",
        "port": 9000,
        "metadata": {
          "debugPort": 0
        }
      }
    },
    "replicaFactor": 1,
    "numShards": 64,
    "isSharded": true,
    "cutoverTime": "0",
    "isMirrored": false,
    "maxShardSetId": 0
  },
  "version": 2
}
```

{{% /tab %}}
{{< /tabs >}}

{{% notice tip %}}
[Read more about the bootstrapping process](/operational_guide/bootstrapping_crash_recovery/).
{{% /notice %}}

### View Details of a Namespace

You can also view the attributes of all namespaces by calling the _{{% apiendpoint %}}namespace_ endpoint

{{< tabs name="check_namespaces" >}}
{{% tab name="Command" %}}

```shell
curl {{% apiendpoint %}}namespace | jq .
```

{{% notice tip %}}
Add `?debug=1` to the request to convert nano units in the output into standard units.
{{% /notice %}}

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "registry": {
    "namespaces": {
      "default": {
        "bootstrapEnabled": true,
        "flushEnabled": true,
        "writesToCommitLog": true,
        "cleanupEnabled": true,
        "repairEnabled": false,
        "retentionOptions": {
          "retentionPeriodNanos": "43200000000000",
          "blockSizeNanos": "1800000000000",
          "bufferFutureNanos": "120000000000",
          "bufferPastNanos": "600000000000",
          "blockDataExpiry": true,
          "blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
          "futureRetentionPeriodNanos": "0"
        },
        "snapshotEnabled": true,
        "indexOptions": {
          "enabled": true,
          "blockSizeNanos": "1800000000000"
        },
        "schemaOptions": null,
        "coldWritesEnabled": false,
        "runtimeOptions": null
      }
    }
  }
}
```

{{% /tab %}}
{{< /tabs >}}

## Writing and Querying Metrics

### Writing Metrics

M3 supports ingesting [statsd](https://github.com/statsd/statsd#usage) and [Prometheus](https://prometheus.io/docs/concepts/data_model/) formatted metrics.

This quickstart focuses on Prometheus metrics which consist of a value, a timestamp, and tags to bring context and meaning to the metric.

You can write metrics using one of two endpoints:

-   _[{{% apiendpoint %}}prom/remote/write](/m3coordinator/api/remote/)_ - Write a Prometheus remote write query to M3DB with a binary snappy compressed Prometheus WriteRequest protobuf message.
-   _{{% apiendpoint %}}json/write_ - Write a JSON payload of metrics data. This endpoint is quick for testing purposes but is not as performant for production usage.

For this quickstart, use the _{{% apiendpoint %}}json/write_ endpoint to write a tagged metric to M3DB with the following data in the request body, all fields are required:

-   `tags`: An object of at least one `name`/`value` pairs
-   `timestamp`: The UNIX timestamp for the data
-   `value`: The value for the data, can be of any type

{{% notice tip %}}
The examples below use `__name__` as the name for one of the tags, which is a Prometheus reserved tag that allows you to query metrics using the value of the tag to filter results.
{{% /notice %}}

{{% notice tip %}}
Label names may contain ASCII letters, numbers, underscores, and Unicode characters. They must match the regex `[a-zA-Z_][a-zA-Z0-9_]*`. Label names beginning with `__` are reserved for internal use. [Read more in the Prometheus documentation](https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels).
{{% /notice %}}

{{< tabs name="write_metrics" >}}
{{< tab name="Command 1" >}}

{{% codeinclude file="includes/quickstart/write-metrics-1.sh" language="shell" %}}

{{< /tab >}}
{{< tab name="Command 2" >}}

{{% codeinclude file="includes/quickstart/write-metrics-2.sh" language="shell" %}}

{{< /tab >}}
{{< tab name="Command 3" >}}

{{% codeinclude file="includes/quickstart/write-metrics-3.sh" language="shell" %}}

{{< /tab >}}
{{< /tabs >}}

### Querying metrics

M3DB supports three query engines: Prometheus (default), Graphite, and the M3 Query Engine.

This quickstart uses Prometheus as the query engine, and you have access to [all the features of PromQL queries](https://prometheus.io/docs/prometheus/latest/querying/basics/).

To query metrics, use the _{{% apiendpoint %}}query_range_ endpoint with the following data in the request body, all fields are required:

-   `query`: A PromQL query
-   `start`: Timestamp in `RFC3339Nano` of start range for results
-   `end`: Timestamp in `RFC3339Nano` of end range for results
-   `step`: A duration or float of the query resolution, the interval between results in the timespan between `start` and `end`.

Below are some examples using the metrics written above.

#### Return results in past 45 seconds

{{< tabs name="example_promql_regex" >}}
{{% tab name="Linux" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue&
  start=$(date "+%s" -d "45 seconds ago")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="macOS/BSD" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue&
  start=$(date -v -45S "+%s")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "third_avenue",
          "checkout": "1",
          "city": "new_york"
        },
        "values": [
          [
            {{% now %}},
            "3347.26"
          ],
          [
            {{% now %}},
            "5347.26"
          ],
          [
            {{% now %}},
            "7347.26"
          ]
        ]
      }
    ]
  }
}
```

{{% /tab %}}
{{< /tabs >}}

#### Values above a certain number

{{< tabs name="example_promql_range" >}}
{{% tab name="Linux" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue > 6000
  start=$(date "+%s" -d "45 seconds ago")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="macOS/BSD" %}}

{{% notice tip %}}
You need to encode the query below.
{{% /notice %}}

```shell
curl -X "POST" "{{% apiendpoint %}}query_range?
  query=third_avenue > 6000
  start=$(date -v -45S "+%s")&
  end=$(date "+%s")&
  step=5s" | jq .
```

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "third_avenue",
          "checkout": "1",
          "city": "new_york"
        },
        "values": [
          [
            {{% now %}},
            "7347.26"
          ]
        ]
      }
    ]
  }
}
```

{{% /tab %}}
{{< /tabs >}}

## Next Steps

This quickstart covered getting a single-node M3DB cluster running, and writing and querying metrics to the cluster. Some next steps are:

-   one
-   two
