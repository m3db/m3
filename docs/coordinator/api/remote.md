# API

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

--8<--
docs/common/headers_optional_read_write.md
--8<--

### Data Params

Binary [snappy compressed](http://google.github.io/snappy/) Prometheus [WriteRequest protobuf message](https://github.com/prometheus/prometheus/blob/10444e8b1dc69ffcddab93f09ba8dfa6a4a2fddb/prompb/remote.proto#L22-L24).

### Available Tuning Params 

**Note:** All relevant parameters can be found under the `queue_config` section of the Prometheus `remote_write` configuration.

`capacity`
Capacity controls how many samples are queued in memory per shard before blocking reading from the WAL. Once the WAL is blocked, samples cannot be appended to any shards and all throughput will cease.

Capacity should be high enough to avoid blocking other shards in most cases, but too much capacity can cause excess memory consumption and longer times to clear queues during resharding. It is recommended to set capacity to 3-10 times `max_samples_per_send`.

`max_shards`
Max shards configures the maximum number of shards, or parallelism, Prometheus will use for each remote write queue. Prometheus will try not to use too many shards,but if the queue falls behind the remote write component will increase the number of shards up to max shards to increase thoughput. Unless remote writing to a very slow endpoint, it is unlikely that `max_shards` should be increased beyond the default. However, it may be necessary to reduce max shards if there is potential to overwhelm the remote endpoint, or to reduce memory usage when data is backed up.

`min_shards`
Min shards configures the minimum number of shards used by Prometheus, and is the number of shards used when remote write starts. If remote write falls behind, Prometheus will automatically scale up the number of shards so most users do not have to adjust this parameter. However, increasing min shards will allow Prometheus to avoid falling behind at the beginning while calculating the required number of shards.

`max_samples_per_send`
Max samples per send can be adjusted depending on the backend in use. Many systems work very well by sending more samples per batch without a significant increase in latency. Other backends will have issues if trying to send a large number of samples in each request. The default value is small enough to work for most systems.

`batch_send_deadline`
Batch send deadline sets the maximum amount of time between sends for a single shard. Even if the queued shards has not reached `max_samples_per_send`, a request will be sent. Batch send deadline can be increased for low volume systems that are not latency sensitive in order to increase request efficiency.

`min_backoff`
Min backoff controls the minimum amount of time to wait before retrying a failed request. Increasing the backoff spreads out requests when a remote endpoint comes back online. The backoff interval is doubled for each failed requests up to `max_backoff`.

`max_backoff`
Max backoff controls the maximum amount of time to wait before retrying a failed request.

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
```bash
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

For more details on querying data in PromQL that was written using this endpoint, see the [query API documentation](../../query_engine/api/).

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

--8<--
docs/common/headers_optional_read_write.md
--8<--

### Data Params

Binary [snappy compressed](http://google.github.io/snappy/) Prometheus [WriteRequest protobuf message](https://github.com/prometheus/prometheus/blob/10444e8b1dc69ffcddab93f09ba8dfa6a4a2fddb/prompb/remote.proto#L26-L28).
