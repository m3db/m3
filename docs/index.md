# M3

## About

After using open-source metrics solutions and finding issues with them at scale – such as reliability, cost, and
operational complexity – [M3](https://github.com/m3db/m3) was created from the ground up to provide Uber with a
native, distributed time series database, a highly-dynamic and performant aggregation service, a query engine, and
other supporting infrastructure.

## Key Features

M3 has several features, provided as discrete components, which make it an ideal platform for time series data at scale:

* A distributed time series database, [M3DB](m3db/index.md), that provides scalable storage for time series data and a reverse index.
* A sidecar process, [M3Coordinator](integrations/prometheus.md), that allows M3DB to act as the long-term storage for Prometheus.
* A distributed query engine, [M3Query](m3query/index.md), with native support for PromQL and Graphite (M3QL coming soon).
<!-- Add M3Aggregator link -->
* An aggregation tier, M3Aggregator, that runs as a dedicated metrics aggregator/downsampler allowing metrics to be stored at various retentions at different resolutions.

## Getting Started

**Note:** Make sure to read our [Operational Guides](operational_guide/index.md) before running in production!

Getting started with M3 is as easy as following one of the How-To guides.

* [Single M3DB node deployment](how_to/single_node.md)
* [Clustered M3DB deployment](how_to/cluster_hard_way.md)
* [M3DB on Kubernetes](how_to/kubernetes.md)
* [Isolated M3Query on deployment](how_to/query.md)

## Support

For support with any issues, questions about M3 or its operation, or to leave any comments, the team can be
reached in a variety of ways:

* [Slack](http://bit.ly/m3slack)
* [Email](https://groups.google.com/forum/#!forum/m3db)
* [Github issues](https://github.com/m3db/m3/issues)
