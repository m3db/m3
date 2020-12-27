## Organizing Data with Placements and Namespaces

A time series database (TSDBs) typically consist of one node (or instance) to store metrics data. This setup is simple to use but has issues with scalability over time as the quantity of metrics data written and read increases.

As a distributed TSDB, M3 helps solve this problem by spreading metrics data, and demand for that data, across multiple nodes in a cluster. M3 does this by splitting data into segments that match certain criteria (such as above a certain value) across nodes into shards.

<!-- TODO: Find an image -->

If you've worked with a distributed database before, then these concepts are probably familiar to you, but M3 uses different terminology to represent some concepts.

-   Every cluster has **one** placement that maps shards to nodes in the cluster.
-   A cluster can have **0 or more** namespaces that are similar conceptually to tables in other databases, and each node serves every namespace for the shards it owns.

<!-- TODO: Image -->

For example, if the cluster placement states that node A owns shards 1, 2, and 3, then node A owns shards 1, 2, 3 for all configured namespaces in the cluster. Each namespace has its own configuration options, including a name and retention time for the data.

## Create a Placement and Namespace

This quickstart uses the _{{% apiendpoint %}}database/create_ endpoint that creates a namespace, and the placement if it doesn't already exist based on the `type` argument.

You can create [placements](/docs/operational_guide/placement_configuration/) and [namespaces](/docs/operational_guide/namespace_configuration/#advanced-hard-way) separately if you need more control over their settings.

In another terminal, use the following command.

{{< tabs name="create_placement_namespace" >}}
{{< tab name="Command" >}}

{{< codeinclude file="docs/includes/create-database.sh" language="shell" >}}

{{< /tab >}}
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

{{% /tab %}}
{{< /tabs >}}

Placement initialization can take a minute or two. Once all the shards have the `AVAILABLE` state, the node has finished bootstrapping, and you should see the following messages in the node console output.

<!-- TODO: Fix these timestamps -->

```shell
{"level":"info","ts":1598367624.0117292,"msg":"bootstrap marking all shards as bootstrapped","namespace":"default","namespace":"default","numShards":64}
{"level":"info","ts":1598367624.0301404,"msg":"bootstrap index with bootstrapped index segments","namespace":"default","numIndexBlocks":0}
{"level":"info","ts":1598367624.0301914,"msg":"bootstrap success","numShards":64,"bootstrapDuration":0.049208827}
{"level":"info","ts":1598367624.03023,"msg":"bootstrapped"}
```

You can check on the status by calling the _{{% apiendpoint %}}services/m3db/placement_ endpoint:

{{< tabs name="check_placement" >}}
{{% tab name="Command" %}}

```shell
curl {{% apiendpoint %}}services/m3db/placement | jq .
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
[Read more about the bootstrapping process](/docs/operational_guide/bootstrapping_crash_recovery/).
{{% /notice %}}

### Ready a Namespace
<!-- TODO: Why?> -->
Once a namespace has finished bootstrapping, you must mark it as ready before receiving traffic by using the _{{% apiendpoint %}}services/m3db/namespace/ready_.

{{< tabs name="ready_namespaces" >}}
{{% tab name="Command" %}}

{{% codeinclude file="docs/includes/quickstart/ready-namespace.sh" language="shell" %}}

{{% /tab %}}
{{% tab name="Output" %}}

```json
{
"ready": true
}
```

{{% /tab %}}
{{< /tabs >}}

### View Details of a Namespace

You can also view the attributes of all namespaces by calling the _{{% apiendpoint %}}services/m3db/namespace_ endpoint

{{< tabs name="check_namespaces" >}}
{{% tab name="Command" %}}

```shell
curl {{% apiendpoint %}}services/m3db/namespace | jq .
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

-   _[{{% apiendpoint %}}prom/remote/write](/docs/m3coordinator/api/remote/)_ - Write a Prometheus remote write query to M3DB with a binary snappy compressed Prometheus WriteRequest protobuf message.
-   _{{% apiendpoint %}}json/write_ - Write a JSON payload of metrics data. This endpoint is quick for testing purposes but is not as performant for production usage.

For this quickstart, use the _{{% apiendpoint %}}json/write_ endpoint to write a tagged metric to M3 with the following data in the request body, all fields are required:

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

{{< codeinclude file="docs/includes/write-metrics-1.sh" language="shell" >}}

{{< /tab >}}
{{< tab name="Command 2" >}}

{{< codeinclude file="docs/includes/write-metrics-2.sh" language="shell" >}}

{{< /tab >}}
{{< tab name="Command 3" >}}

{{< codeinclude file="docs/includes/write-metrics-3.sh" language="shell" >}}

{{< /tab >}}
{{< /tabs >}}

### Querying metrics

M3 supports three query engines: Prometheus (default), Graphite, and the M3 Query Engine.

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

<!-- TODO: Check this on Linux -->

```shell
curl -X "POST" -G "{{% apiendpoint %}}query_range" \
  -d "query=third_avenue" \
  -d "start=$(date "+%s" -d "45 seconds ago")" \
  -d "end=$( date +%s )" \
  -d "step=5s" | jq .  
```

{{% /tab %}}
{{% tab name="macOS/BSD" %}}

```shell
curl -X "POST" -G "{{% apiendpoint %}}query_range" \
  -d "query=third_avenue" \
  -d "start=$( date -v -45S +%s )" \
  -d "end=$( date +%s )" \
  -d "step=5s" | jq .
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

<!-- TODO: Check Linux command -->

```shell
curl -X "POST" -G "{{% apiendpoint %}}query_range" \
  -d "query=third_avenue > 6000" \
  -d "start=$(date "+%s" -d "45 seconds ago")" \
  -d "end=$( date +%s )" \
  -d "step=5s" | jq .
```

{{% /tab %}}
{{% tab name="macOS/BSD" %}}

```shell
curl -X "POST" -G "{{% apiendpoint %}}query_range" \
  -d "query=third_avenue > 6000" \
  -d "start=$(date -v -45S "+%s")" \
  -d "end=$( date +%s )" \
  -d "step=5s" | jq .
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

<!-- ## Next Steps

This quickstart covered getting a single-node M3DB cluster running, and writing and querying metrics to the cluster. Some next steps are:

-   one
-   two -->
