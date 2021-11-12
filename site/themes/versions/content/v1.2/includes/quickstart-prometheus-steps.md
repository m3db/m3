This quickstart uses [the textfile collector feature](https://github.com/prometheus/node_exporter#textfile-collector) of the Prometheus node exporter to export metrics to Prometheus that M3 then ingests. To follow the next steps, [download node_exporter](https://github.com/prometheus/node_exporter#installation-and-usage).

#### Configure and Start Prometheus

With M3 running and ready to receive metrics, change your Prometheus configuration to add M3 as `remote_read` and `remote_write` URLs, and as a job. With the configuration below, Prometheus scrapes metrics from two local nodes `localhost:8080` and `localhost:8081` in the `production` group, and one local node `localhost:8082` in the `canary` group:

{{< codeinclude file="docs/includes/prometheus.yml" language="yaml" >}}

Start Prometheus using the new configuration with the command below:

```shell
prometheus --config.file=prometheus.yml
```

#### Start Node Exporters

The three commands below simulate three point of sale (POS) devices reporting an hourly sales total for the POS. The node_exporter textfile collector uses[ _.prom_ files](https://prometheus.io/docs/instrumenting/exposition_formats/) for metrics, and only loads files from a directory, so this requires some extra steps for each node for this example.

{{< tabs name="node_exporters" >}}
{{< tab name="Node 1" >}}

{{% notice tip %}}
Below is the text file for the first node, copy and paste it into a new _.prom_ file in a directory named _node-1_, or [download the file](/docs/includes/quickstart/node-1/metrics-1.prom) into that new folder.
{{% /notice %}}

{{< codeinclude file="docs/includes/quickstart/node-1/metrics-1.prom" language="text" >}}

Run node_exporter from its install location, passing the directory that contains the textfile, and the public facing address, which for this example serves as the identifier of the POS.

{{< codeinclude file="docs/includes/quickstart/node-1/node_exporter-1.sh" language="shell" >}}

{{< /tab >}}
{{< tab name="Node 2" >}}

{{% notice tip %}}
Below is the text file for the second node, copy and paste it into a new _.prom_ file in a directory named _node-2_, or [download the file](/docs/includes/quickstart/node-2/metrics-2.prom) into that new folder.
{{% /notice %}}

{{< codeinclude file="docs/includes/quickstart/node-2/metrics-2.prom" language="text" >}}

Run node_exporter from its install location, passing the directory that contains the textfile, and the public facing address, which for this example serves as the identifier of the POS.

{{< codeinclude file="docs/includes/quickstart/node-2/node_exporter-2.sh" language="shell" >}}

{{< /tab >}}
{{< tab name="Node 3" >}}

{{% notice tip %}}
Below is the text file for the second node, copy and paste it into a new _.prom_ file in a directory named _node-3_, or [download the file](/docs/includes/quickstart/node-3/metrics-3.prom) into that new folder.
{{% /notice %}}

{{< codeinclude file="docs/includes/quickstart/node-3/metrics-3.prom" language="text" >}}

Run node_exporter from it's install location, passing the directory that contains the textfile, and the public facing address, which for this example serves as the identifier of the POS.

{{< codeinclude file="docs/includes/quickstart/node-3/node_exporter-3.sh" language="shell" >}}

{{< /tab >}}
{{< /tabs >}}

{{% notice tip %}}
You can now confirm that the node_exporter exported metrics to Prometheus by searching for `third_avenue` in the Prometheus dashboard.
{{% /notice %}}
