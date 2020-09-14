---
title: "Integrating with Graphite"
date: 2020-04-21T20:52:20-04:00
draft: true
---

### Graphite
This document is a getting started guide to integrating the M3 stack with Graphite.

### Overview
M3 supports ingesting Graphite metrics using the Carbon plaintext protocol. We also support a variety of aggregation and storage policies for the ingestion pathway (similar to storage-schemas.conf when using Graphite Carbon) that are documented below. Finally, on the query side, we support the majority of graphite query functions.

#### Ingestion
Setting up the M3 stack to ingest carbon metrics is straightforward. First, make sure you've followed our other documentation to get m3coordinator and M3DB setup. Also, familiarize yourself with how M3 handles aggregation.

Once you have both of those services running properly, modify your m3coordinator configuration to add the following lines and restart it:
carbon:
  ingester:
    listenAddress: "0.0.0.0:7204"

This will enable a line-based TCP carbon ingestion server on the specified port. By default, the server will write all carbon metrics to every aggregated namespace specified in the m3coordinator configuration file and aggregate them using a default strategy of mean (equivalent to Graphite's Average).
This default setup makes sense if your carbon metrics are unaggregated, however, if you've already aggregated your data using something like statsite then you may want to disable M3 aggregation. In that case, you can do something like the following:
carbon:
  ingester:
    listenAddress: "0.0.0.0:7204"
    rules:
      - pattern: .*
        aggregation:
          enabled: false
        policies:
          - resolution: 1m
            retention: 48h

This replaces M3's default behavior with a single rule which states that all metrics (since .* will match any string) should be written to whichever aggregated M3DB namespace has been configured with a resolution of 1 minute and a retention of 48 hours, bypassing aggregation / downsampling altogether. Note that there must be a configured M3DB namespace with the specified resolution/retention or the coordinator will fail to start.
In the situation that you choose to use M3's aggregation functionality, there are a variety of aggregation types you can choose from. For example:
carbon:
  ingester:
    listenAddress: "0.0.0.0:7204"
    rules:
      - pattern: .*
        aggregation:
          type: last
        policies:
          - resolution: 1m
            retention: 48h

The config above will aggregate ingested carbon metrics into 1 minute tiles, but instead of taking the mean of every datapoint, it will emit the last datapoint that was received within a given tile's window.
Similar to Graphite's storage-schemas.conf, M3 carbon ingestion rules are applied in order and only the first pattern that matches is applied. In addition, the rules can be as simple or as complex as you like. For example:
carbon:
  ingester:
    listenAddress: "0.0.0.0:7204"
    rules:
      - pattern: stats.internal.financial-service.*
        aggregation:
          type: max
        policies:
          - resolution: 1m
            retention: 4320h
          - resolution: 10s
            retention: 24h
      - pattern: stats.internal.rest-proxy.*
        aggregation:
          type: mean
        policies:
          - resolution: 10s
            retention: 2h
      - pattern: stats.cloud.*
        aggregation:
          enabled: false
        policies:
          - resolution: 1m
            retention: 2h
      - pattern: .*
        aggregation:
          type: mean
        policies:
          - resolution: 1m
            retention: 48h

Lets break that down.
The first rule states that any metric matching the pattern stats.internal.financial-service.* should be aggregated using the max function (meaning the datapoint with the highest value that is received in a given window will be retained) to generate two different tiles, one with 1 minute resolution and another with 10 second resolution which will be written out to M3DB namespaces with 4320 hour and 24 hour retentions respectively.

The second rule will aggregate all the metrics coming from our rest-proxy service using a mean type aggregation (all datapoints within a given window will be averaged) to generate 10 second tiles and write them out to an M3DB namespace that stores data for two hours.

The third will match any metrics coming from our cloud environment. In this hypoethical example, our cloud metrics are already aggregated using an application like statsite, so instead of aggregating them again, we just write them directly to an M3DB namespace that retains data for two hours. Note that while we're not aggregating the data in M3 here, we still need to provide a resolution so that the ingester can match the storage policy to a known M3DB namespace, as well as so that when we fan out queries to multiple namespaces we know the resolution of the data contained in each namespace.

Finally, our last rule uses a "catch-all" pattern to capture any metrics that don't match any of our other rules and aggregate them using the mean function into 1 minute tiles which we store for 48 hours.

#### Debug mode
If at any time you're not sure which metrics are being matched by which patterns, or want more visibility into how the carbon ingestion rule are being evaluated, modify the config to enable debug mode:
carbon:
  ingester:
    debug: true
    listenAddress: "0.0.0.0:7204"

This will make the carbon ingestion emit logs for every step that is taking. Note: If your coordinator is ingesting a lot of data, enabling this mode could bring the proccess to a halt due to the I/O overhead, so use this feature cautiously in production environments.

Supported Aggregation Functions
last
min
max
mean
median
count
sum
sumsq
stdev
p10
p20
p30
p40
p50
p60
p70
p80
p90
p95
p99
p999
p9999
