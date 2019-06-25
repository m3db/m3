# API

**Please note:** This documentation is a work in progress and more detail is required.

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

--8<--
docs/common/headers_optional_read_write.md
--8<--

### Data Params

Binary [snappy compressed](https://en.wikipedia.org/wiki/Snappy_(compression)) Prometheus [WriteRequest protobuf message](https://github.com/prometheus/prometheus/blob/10444e8b1dc69ffcddab93f09ba8dfa6a4a2fddb/prompb/remote.proto#L22).

### Sample Call

There is no trivial way to Snappy compress and marshal a Prometheus WriteRequest protobuf message using just shell, so this example uses a specific command line utility instead. 

This sample call is made using `promremotecli` which is a command line tool that uses a [Go client](https://github.com/m3db/prometheus_remote_client_golang) to Prometheus Remote endpoints. For more information visit the [GitHub repository](https://github.com/m3db/prometheus_remote_client_golang).

There is also a [Java client](https://github.com/m3dbx/prometheus_remote_client_java) that can be used to make requests to the endpoint.

Each `-t` parameter specifies a label (dimension) to add to the metric.

The `-h` parameter can be used as many times as necessary to add headers to the outgoing request in the form of "Header-Name: HeaderValue".

Here is an example of writing the datapoint at the current unix timestamp with value 123.456:

<!-- 
Note: keep this example similar to the one found in query API 
documentation for consistency/ease of readers.
-->
```bash
docker run -it --rm                                            \
  quay.io/m3db/prometheus_remote_client_golang:latest          \
  -u http://host.docker.internal:7201/api/v1/prom/remote/write \
  -t __name__:http_requests_total                              \
  -t code:200                                                  \
  -t handler:graph                                             \
  -t method:get                                                \
  -d $(date +"%s"),123.456

# You can also use the following docker image for paranoia about the image being published:
# quay.io/m3db/prometheus_remote_client_golang@sha256:fc56df819bff9a5a087484804acf3a584dd4a78c68900c31a28896ed66ca7e7b
```
