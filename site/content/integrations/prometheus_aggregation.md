---
title: "Prometheus: Aggregation for Prometheus, Thanos or other remote write storage with M3"
weight: 2
---

This document is a getting started guide to using M3 Coordinator or both 
M3 Coordinator and M3 Aggregator roles to aggregate metrics for a compatible 
Prometheus remote write storage backend.

What's required is any Prometheus storage backend that supports the [Prometheus 
Remote write protocol](https://docs.google.com/document/d/1LPhVRSFkGNSuU1fBd81ulhsCPR4hkSZyyBj1SZ8fWOM/).

## Testing with docker compose

To test out a full end-to-end example you can clone the M3 repository and use the corresponding guide for the [M3 and Prometheus remote write stack docker compose development stack](https://github.com/m3db/m3/blob/master/scripts/development/m3_prom_remote_stack/).

## Basic guide with single M3 Coordinator sidecar aggregation

Start by downloading the [M3 Coordinator config template](https://github.com/m3db/m3/blob/91db5e12cd34a95658cc00fa44ed9ae14d512710/src/query/config/m3coordinator-prom-remote-template.yml).

Update the endpoints with your Prometheus Remote Write compatible storage setup. You should endup with config similar to:

```yaml
backend: prom-remote

prometheusRemoteBackend:
  endpoints:
    # This points to a Prometheus started with `--storage.tsdb.retention.time=720h`
    - name: unaggregated
      address: "http://prometheus-raw:9090/api/v1/write"
    # This points to a Prometheus started with `--storage.tsdb.retention.time=1440h`      
    - name: aggregated
      address: "http://prometheus-agg:9090/api/v1/write"
      storagePolicy:
        # Should match retention of a Prometheus instance. Coordinator uses it for routing metrics correctly.
        retention: 1440h
        # Resolution instructs M3Aggregator to downsample incoming metrics at given rate.
        # By tuning resolution we can control how much storage Prometheus needs at the cost of query accuracy as range shrinks.
        resolution: 5m
    # Another example of Prometheus configured for a very long retention but with 1h resolution
    # Because of downsample: all == false metrics are downsampled based on mapping and rollup rules.     
    - name: historical
      address: "http://prometheus-hist:9090/api/v1/write"
      storagePolicy:
        retention: 8760h
        resolution: 1h
        downsample:
          all: false
```

## More advanced deployments

Refer to the [M3 Aggregation for any Prometheus remote write storage](/docs/how_to/any_remote_storage) for more details on more advanced deployment options.
