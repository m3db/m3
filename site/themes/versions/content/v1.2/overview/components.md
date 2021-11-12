---
title: "Components"
weight: 1
---

## M3 Coordinator

M3 Coordinator is a service that coordinates reads and writes between upstream systems, such as Prometheus, and M3DB. It is a bridge that users can deploy to access the benefits of M3DB such as long term storage and multi-DC setup with other monitoring systems, such as Prometheus. See [this presentation](http://schd.ws/hosted_files/cloudnativeeu2017/73/Integrating%20Long-Term%20Storage%20with%20Prometheus%20-%20CloudNativeCon%20Berlin%2C%20March%2030%2C%202017.pdf) for more on long term storage in Prometheus.

## M3DB

M3DB is a distributed time series database that provides scalable storage and a reverse index of time series. It is optimized as a cost effective and reliable realtime and long term retention metrics store and index.  For more details, see the [M3DB documentation](/docs/reference/m3db/).

## M3 Query

{{< fileinclude "docs/includes/m3query_intro.md" >}}

## M3 Aggregator

{{< fileinclude "docs/includes/m3aggregator_intro.md" >}}