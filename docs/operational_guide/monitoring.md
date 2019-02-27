## Metrics

TODO: document how to retrieve metrics for M3DB components.

## Logs

TODO: document how to retrieve logs for M3DB components.

## Tracing

M3DB is integrated with [opentracing](https://opentracing.io/) to provide
insight into query performance and errors.

### Configuration
Currently, only [Jaeger](https://www.jaegertracing.io/) is supported as a backend.

To enable it, set tracing.backend to "jaeger" (see also our
[sample local config](https://github.com/m3db/m3/blob/master/src/query/config/m3query-local-etcd.yml):

```
tracing:
    backend: jaeger  # enables jaeger with default configs
    jaeger:
        # optional configuration for jaeger -- see
        # https://github.com/jaegertracing/jaeger-client-go/blob/master/config/config.go#L37
        # for options
        ...
```

Jaeger can be run locally with docker as described in
https://www.jaegertracing.io/docs/1.9/getting-started/.

The default configuration will report traces via udp to localhost:6831;
using the all-in-one jaeger container, they will be accessible at

http://localhost:16686

N.B.: for production workloads, you will almost certainly want to use
sampler.type=remote with 
[adaptive sampling](https://www.jaegertracing.io/docs/1.10/sampling/#adaptive-sampler)
for Jaeger, as write volumes are likely orders of magnitude higher than
read volumes in most timeseries systems.

#### Alternative backends

If you'd like additional backends, we'd love to support them!

File an issue against M3 and we can work with you on how best to add
the backend. The first time's going to be a little rough--opentracing
unfortunately doesn't support Go plugins (yet--see
https://github.com/opentracing/opentracing-go/issues/133), and `glide`'s
update model means that adding dependencies directly will update
*everything*, which isn't ideal for an isolated dependency change.
These problems are all solvable though,
and we'll work with you to make it happen!

### Use cases

Note: all URLs assume a local jaeger setup as described in Jaeger's
[docs](https://www.jaegertracing.io/docs/1.9/getting-started/).


#### Finding slow queries

To find prom queries longer than <threshold>, filter for `minDuration >= <threshold>` on
`operation="GET /api/v1/query_range"`.

Sample query:
http://localhost:16686/search?end=1548876672544000&limit=20&lookback=1h&maxDuration&minDuration=1ms&operation=GET%20%2Fapi%2Fv1%2Fquery_range&service=m3query&start=1548873072544000

#### Finding queries with errors

Search for `error=true` on `operation="GET /api/v1/query_range"`
http://localhost:16686/search?operation=GET%20%2Fapi%2Fv1%2Fquery_range&service=m3query&tags=%7B%22error%22%3A%22true%22%7D

#### Finding 500 (Internal Server Error) responses

Search for `http.status_code=500`.

http://localhost:16686/search?limit=20&lookback=24h&maxDuration&minDuration&operation=GET%20%2Fapi%2Fv1%2Fquery_range&service=m3query&start=1548802430108000&tags=%7B"http.status_code"%3A"500"%7D