# M3

## About

[M3](https://github.com/m3db/m3) is an open source metrics platform that was developed at Uber out of necessity.
After using what was available as open source and finding we were unable to use them at our scale due to issues
with their reliability, cost and operationally intensive nature, we built our own metrics platform piece by piece.
We used our experience to help us build a native distributed time series database, a highly dynamic and performant
aggregation service, query engine and other supporting infrastructure.

## Key Features

Below are some of the key features of M3 that make it a great platform for time series data.

* Distributed time series database (M3DB) that provides scalable storage and a reverse index of time series.
* Side-car process (M3Coordinator) that allows M3DB to act as the long-term storage for Prometheus.
* Distributed query engine (M3Query) with native support for PromQL and Graphite.
* Aggregation tier (M3Aggregator) that runs as a dedicated metrics aggregator.

## Getting Started

Getting started with M3 is as easy as following one of our `How-To` guides.

* [Single M3DB node deployment](how_to/single_node.md)
* [Clustered M3DB deployment](how_to/cluster_hard_way.md)
* [M3DB on Kubernetes](how_to/kubernetes.md)
* [Isolated M3Query on deployment](how_to/query.md)

## Support

Should you run into any issues, have questions about M3, or want to leave any comments, you can reach us
on a number of different channels.

* [Gitter](https://gitter.im/m3db/Lobby)
* [Email](https://groups.google.com/forum/#!forum/m3db)
* [Github issues](https://github.com/m3db/m3/issues)
