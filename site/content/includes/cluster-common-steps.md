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
