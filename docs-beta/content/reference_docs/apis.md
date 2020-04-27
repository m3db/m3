---
title: "APIs"
date: 2020-04-21T21:02:36-04:00
draft: true
---

M3 Coordinator, API for reading/writing metrics and M3 management
M3 Coordinator is a service that coordinates reads and writes between upstream systems, such as Prometheus, and downstream systems, such as M3DB.
It also provides management APIs to setup and configure different parts of M3.
The coordinator is generally a bridge for read and writing different types of metrics formats and a management layer for M3.

API
The M3 Coordinator implements the Prometheus Remote Read and Write HTTP endpoints, they also can be used however as general purpose metrics write and read APIs. Any metrics that are written to the remote write API can be queried using PromQL through the query APIs as well as being able to be read back by the Prometheus Remote Read endpoint.
Remote Write
Write a Prometheus Remote write query to M3.
URL
/api/v1/prom/remote/write
Method
POST
URL Params
None.
Header Params
Optional
M3-Metrics-Type:
If this header is set, it determines what type of metric to store this metric value as. Otherwise by default, metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting downsample options to all: false for a namespace in the coordinator config, for more see disabling automatic aggregation.

Must be one of:
unaggregated: Write metrics directly to configured unaggregated namespace.
aggregated: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the M3-Storage-Policy header to be set to resolve which namespace to write metrics to.


M3-Storage-Policy:
If this header is set, it determines which aggregated namespace to read/write metrics directly to/from (bypassing any aggregation).
The value of the header must be in the format of resolution:retention in duration shorthand. e.g. 1m:48h specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

Here is an example of querying metrics from a specific namespace.
Data Params
Binary snappy compressed Prometheus WriteRequest protobuf message.
Available Tuning Params
Refer here for an up to date list of remote tuning parameters.
Sample Call
There isn't a straightforward way to Snappy compress and marshal a Prometheus WriteRequest protobuf message using just shell, so this example uses a specific command line utility instead.
This sample call is made using promremotecli which is a command line tool that uses a Go client to Prometheus Remote endpoints. For more information visit the GitHub repository.
There is also a Java client that can be used to make requests to the endpoint.
Each -t parameter specifies a label (dimension) to add to the metric.
The -h parameter can be used as many times as necessary to add headers to the outgoing request in the form of "Header-Name: HeaderValue".
Here is an example of writing the datapoint at the current unix timestamp with value 123.456:
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

For more details on querying data in PromQL that was written using this endpoint, see the query API documentation.
Remote Read
Read Prometheus metrics from M3.
URL
/api/v1/prom/remote/read
Method
POST
URL Params
None.
Header Params
Optional
M3-Metrics-Type:
If this header is set, it determines what type of metric to store this metric value as. Otherwise by default, metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting downsample options to all: false for a namespace in the coordinator config, for more see disabling automatic aggregation.

Must be one of:
unaggregated: Write metrics directly to configured unaggregated namespace.
aggregated: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the M3-Storage-Policy header to be set to resolve which namespace to write metrics to.


M3-Storage-Policy:
If this header is set, it determines which aggregated namespace to read/write metrics directly to/from (bypassing any aggregation).
The value of the header must be in the format of resolution:retention in duration shorthand. e.g. 1m:48h specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

Here is an example of querying metrics from a specific namespace.
Data Params
Binary snappy compressed Prometheus WriteRequest protobuf message.

Query Engine

API
Please note: This documentation is a work in progress and more detail is required.
Query using PromQL
Query using PromQL and returns JSON datapoints compatible with the Prometheus Grafana plugin.
URL
/api/v1/query_range
Method
GET
URL Params
Required
start=[time in RFC3339Nano]
end=[time in RFC3339Nano]
step=[time duration]
target=[string]
Optional
debug=[bool]
lookback=[string|time duration]: This sets the per request lookback duration to something other than the default set in config, can either be a time duration or the string "step" which sets the lookback to the same as the step request parameter.
Header Params
Optional
M3-Metrics-Type:
If this header is set, it determines what type of metric to store this metric value as. Otherwise by default, metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting downsample options to all: false for a namespace in the coordinator config, for more see disabling automatic aggregation.

Must be one of:
unaggregated: Write metrics directly to configured unaggregated namespace.
aggregated: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the M3-Storage-Policy header to be set to resolve which namespace to write metrics to.


M3-Storage-Policy:
If this header is set, it determines which aggregated namespace to read/write metrics directly to/from (bypassing any aggregation).
The value of the header must be in the format of resolution:retention in duration shorthand. e.g. 1m:48h specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

Here is an example of querying metrics from a specific namespace.
Tag Mutation
The M3-Map-Tags-JSON header enables dynamically mutating tags in Prometheus write request. See 2254 for more background.
Currently only write is supported. As an example, the following header would unconditionally cause globaltag=somevalue to be added to all metrics in a write request:
M3-Map-Tags-JSON: '{"tagMappers":[{"write":{"tag":"globaltag","value":"somevalue"}}]}'

Data Params
None.
Sample Call
curl 'http://localhost:7201/api/v1/query_range?query=abs(http_requests_total)&start=1530220860&end=1530220900&step=15s'
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "code": "200",
          "handler": "graph",
          "method": "get"
        },
        "values": [
          [
            1530220860,
            "6"
          ],
          [
            1530220875,
            "6"
          ],
          [
            1530220890,
            "6"
          ]
        ]
      },
      {
        "metric": {
          "code": "200",
          "handler": "label_values",
          "method": "get"
        },
        "values": [
          [
            1530220860,
            "6"
          ],
          [
            1530220875,
            "6"
          ],
          [
            1530220890,
            "6"
          ]
        ]
      }
    ]
  }
}
