---
title: "API"
weight: 1
---

The M3 Coordinator implements the Prometheus Remote Read and Write HTTP endpoints, they also can be used however as general purpose metrics write and read APIs. Any metrics that are written to the remote write API can be queried using PromQL through the query APIs as well as being able to be read back by the Prometheus Remote Read endpoint.

## Remote Write

Write a Prometheus Remote write query to M3.

### URL

`/api/v1/prom/remote/write`

### Method

`POST`

### URL Params

None.

### Header Params

#### Optional

{{% fileinclude "docs/includes/headers_optional_read_write_all.md" %}}

{{% fileinclude "docs/includes/headers_optional_write_all.md" %}}

### Data Params

Binary [snappy compressed](http://google.github.io/snappy/) Prometheus [WriteRequest protobuf message](https://github.com/prometheus/prometheus/blob/10444e8b1dc69ffcddab93f09ba8dfa6a4a2fddb/prompb/remote.proto#L22-L24).

### Available Tuning Params

Refer [here](https://prometheus.io/docs/practices/remote_write/) for an up to date list of remote tuning parameters. 

### Sample Call

There isn't a straightforward way to Snappy compress and marshal a Prometheus WriteRequest protobuf message using just shell, so this example uses a specific command line utility instead. 

This sample call is made using `promremotecli` which is a command line tool that uses a [Go client](https://github.com/m3db/prometheus_remote_client_golang) to Prometheus Remote endpoints. For more information visit the [GitHub repository](https://github.com/m3db/prometheus_remote_client_golang).

There is also a [Java client](https://github.com/m3dbx/prometheus_remote_client_java) that can be used to make requests to the endpoint.

Each `-t` parameter specifies a label (dimension) to add to the metric.

The `-h` parameter can be used as many times as necessary to add headers to the outgoing request in the form of "Header-Name: HeaderValue".

Here is an example of writing the datapoint at the current unix timestamp with value 123.456:

<!-- 
Note: keep this example similar to the one found in query API 
documentation for consistency/ease of readers.
-->

```shell
docker run -it --rm                                            \
  quay.io/m3db/prometheus_remote_client_golang:latest          \
  -u http://host.docker.internal:7201/api/v1/prom/remote/write \
  -t __name__:http_requests_total                              \
  -t code:200                                                  \
  -t handler:graph                                             \
  -t method:get                                                \
  -d $(date +"%s"),123.456
promremotecli_log 2019/06/25 04:13:56 writing datapoint [2019-06-25 04:13:55 +0000 UTC 123.456]
promremotecli_log 2019/06/25 04:13:56 labelled [[__name__ http_requests_total] [code 200] [handler graph] [method get]]
promremotecli_log 2019/06/25 04:13:56 writing to http://host.docker.internal:7201/api/v1/prom/remote/write
{"success":true,"statusCode":200}
promremotecli_log 2019/06/25 04:13:56 write success

# If you are paranoid about image tags being hijacked/replaced with nefarious code, you can use this SHA256 tag:
# quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b
```

For more details on querying data in PromQL that was written using this endpoint, see the [query API documentation](/v0.15.17/docs/m3query/api/query).

## Remote Read

Read Prometheus metrics from M3.

### URL

`/api/v1/prom/remote/read`

### Method

`POST`

### URL Params

None.

### Header Params

#### Optional

{{% fileinclude "docs/includes/headers_optional_read_write_all.md" %}}

{{% fileinclude "docs/includes/headers_optional_read_all.md" %}}

### Data Params

Binary [snappy compressed](http://google.github.io/snappy/) Prometheus [WriteRequest protobuf message](https://github.com/prometheus/prometheus/blob/10444e8b1dc69ffcddab93f09ba8dfa6a4a2fddb/prompb/remote.proto#L26-L28).
