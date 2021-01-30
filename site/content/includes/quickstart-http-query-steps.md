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
