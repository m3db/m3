---
title: "Tracing"
date: 2020-04-21T20:56:40-04:00
draft: true
---

### Tracing
M3DB is integrated with opentracing to provide insight into query performance and errors.

#### Jaeger
To enable Jaeger as the tracing backend, set tracing.backend to "jaeger" (see also our sample local config:
tracing:
    backend: jaeger  # enables jaeger with default configs
    jaeger:
        # optional configuration for jaeger -- see
        # https://github.com/jaegertracing/jaeger-client-go/blob/master/config/config.go#L37
        # for options
        ...

Jaeger can be run locally with docker as described in https://www.jaegertracing.io/docs/1.9/getting-started/.

The default configuration will report traces via udp to localhost:6831; using the all-in-one jaeger container, they will be accessible at
http://localhost:16686

N.B.: for production workloads, you will almost certainly want to use sampler.type=remote with adaptive sampling for Jaeger, as write volumes are likely orders of magnitude higher than read volumes in most timeseries systems.

#### LightStep
To use LightStep as the tracing backend, set tracing.backend to "lightstep" and configure necessary information for your client under lightstep. Any options exposed in lightstep-tracer-go can be set in config. Any environment variables may be interpolated. For example:
tracing:
  serviceName: m3coordinator
  backend: lightstep
  lightstep:
    access_token: ${LIGHTSTEP_ACCESS_TOKEN:""}
    collector:
      scheme: https
      host: my-satellite-address.domain
      port: 8181

#### Alternative backends
If you'd like additional backends, we'd love to support them!
File an issue against M3 and we can work with you on how best to add the backend. The first time's going to be a little rough--opentracing unfortunately doesn't support Go plugins (yet--see https://github.com/opentracing/opentracing-go/issues/133), and glide's update model means that adding dependencies directly will update everything, which isn't ideal for an isolated dependency change. These problems are all solvable though, and we'll work with you to make it happen!

#### Use cases
Note: all URLs assume a local jaeger setup as described in Jaeger's docs.

**Finding slow queries**
To find prom queries longer than , filter for minDuration >= <threshold> on operation="GET /api/v1/query_range".
Sample query: http://localhost:16686/search?end=1548876672544000&limit=20&lookback=1h&maxDuration&minDuration=1ms&operation=GET%20%2Fapi%2Fv1%2Fquery_range&service=m3query&start=1548873072544000

**Finding queries with errors**
Search for error=true on operation="GET /api/v1/query_range" http://localhost:16686/search?operation=GET%20%2Fapi%2Fv1%2Fquery_range&service=m3query&tags=%7B%22error%22%3A%22true%22%7D

**Finding 500 (Internal Server Error) responses**
Search for http.status_code=500.
http://localhost:16686/search?limit=20&lookback=24h&maxDuration&minDuration&operation=GET%20%2Fapi%2Fv1%2Fquery_range&service=m3query&start=1548802430108000&tags=%7B"http.status_code"%3A"500"%
