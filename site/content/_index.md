---
title: M3 Documentation
weight: 1
---


## About

After using open-source metrics solutions and finding issues with them at scale – such as reliability, cost, and
operational complexity – [M3](https://github.com/m3db/m3) was created from the ground up to provide Uber with a
native, distributed time series database, a highly-dynamic and performant aggregation service, a query engine, and
other supporting infrastructure.

## Key Features

M3 has several features, provided as discrete components, which make it an ideal platform for time series data at scale:

-   A distributed time series database, [M3DB](/docs/m3db/), that provides scalable storage for time series data and a reverse index.
-   A sidecar process, [M3Coordinator](/docs/integrations/prometheus), that allows M3DB to act as the long-term storage for Prometheus.
-   A distributed query engine, [M3Query](/docs/m3query), with native support for PromQL and Graphite (M3QL coming soon).
    <!-- Add M3Aggregator link -->
-   An aggregation tier, M3Aggregator, that runs as a dedicated metrics aggregator/downsampler allowing metrics to be stored at various retentions at different resolutions.

## Getting Started

**Note:** Make sure to read our [Operational Guides](/docs/operational_guide) before running in production!

Getting started with M3 is as easy as following one of the How-To guides.

-   [Single M3DB node deployment](/docs/quickstart)
-   [Clustered M3DB deployment](/docs/cluster)
-   [M3DB on Kubernetes](/docs/operator)
-   [Isolated M3Query on deployment](/docs/how_to/query)

## Support

For support with any issues, questions about M3 or its operation, or to leave any comments, the team can be
reached in a variety of ways:

-   [Slack (main chat channel)](http://bit.ly/m3slack)
-   [Email](https://groups.google.com/forum/#!forum/m3db)
-   [Github issues](https://github.com/m3db/m3/issues)
