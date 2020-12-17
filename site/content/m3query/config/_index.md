---
title: "Configuration"
weight: 1
chapter: true
---


## Default query engine

By default M3 runs two query engines:

- Prometheus (default) - robust and de-facto query language for metrics
- M3 Query Engine - high-performance query engine but doesn't support all the functions yet

Prometheus Query Engine is the default one when calling query endpoint:
```
http://localhost:7201/api/v1/query?query=count(http_requests)&time=1590147165
```

But you can switch between the two in the following ways:

- Changing default query engine in config file (see `defaultEngine` parameter in [Configuration](annotated_config))
- Passing HTTP header `M3-Engine`:

    ```curl -H "M3-Engine: m3query" "http://localhost:7201/api/v1/query?query=count(http_requests)&time=1590147165"```

    or

    ```curl -H "M3-Engine: prometheus" "http://localhost:7201/api/v1/query?query=count(http_requests)&time=1590147165"```

- Passing HTTP query URL parameter `engine`:

    ```curl "http://localhost:7201/api/v1/query?engine=m3query&query=count(http_requests)&time=1590147165"```
    
    or

    ```curl "http://localhost:7201/api/v1/query?engine=prometheus&query=count(http_requests)&time=1590147165"```

- Using different URLs:
    - `/prometheus/api/v1/*` - to call Prometheus query engine
    - `/m3query/api/v1/*` - to call M3 Query query engine

    ```curl "http://localhost:7201/m3query/api/v1/query?query=count(http_requests)&time=1590147165"```
    
    or

    ```curl "http://localhost:7201/prometheus/api/v1/query?query=count(http_requests)&time=1590147165"```
