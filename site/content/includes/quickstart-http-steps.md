You can use the _{{% apiendpoint %}}json/write_ endpoint to write a tagged metric to M3 with the following data in the request body, all fields are required:

-   `tags`: An object of at least one `name`/`value` pairs
-   `timestamp`: The UNIX timestamp for the data
-   `value`: The float64 value for the data

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