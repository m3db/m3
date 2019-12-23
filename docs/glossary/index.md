# Glossary

- **Bootstrapping**: Process by which an M3DB node is brought up. Bootstrapping consists of determining
the integrity of data that the node has, replay writes from the commit log, and/or stream missing data
from its peers.

- **Cardinality**: The number of unique metrics within the M3DB index. Cardinality increases with the
number of unique tag/value combinations that are being emitted. 

- **Datapoint**: A single timestamp/value. Timeseries are composed of multiple datapoints and a series
of tag/value pairs. 

- **Labels**: Pairs of descriptive words that give meaning to a metric. `Tags` and `Labels` are interchangeable terms.

- **Metric**: A collection of uniquely identifiable tags.

- **M3**: Highly scalable, distributed metrics platform that is comprised of a native, distributed time
series database, a highly-dynamic and performant aggregation service, a query engine, and other
supporting infrastructure.

- **M3Coordinator**: A service within M3 that coordinates reads and writes between upstream systems,
such as Prometheus, and downstream systems, such as M3DB.

- **M3DB**: Distributed time series database influenced by [Gorilla](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)
and [Cassandra](http://cassandra.apache.org/) released as open source by Uber Technologies.

- **M3Query**: A distributed query engine for M3DB. Unlike M3Coordinator, M3Query only provides supports for reads.

- **Namespace**: Similar to a table in other types of databases, namespaces in M3DB have a unique name
and a set of configuration options, such as data retention and block size.

- **Placement**: Map of the M3DB cluster's shard replicas to nodes. Each M3DB cluster has only one placement.
`Placement` and `Topology` are interchangeable terms.

- **Shard**: Effectively the same as a "virtual shard" in Cassandra in that it provides an arbitrary
distribution of time series data via a simple hash of the series ID.

- **Tags**: Pairs of descriptive words that give meaning to a metric. `Tags` and `Labels` are interchangeable terms.

- **Timeseries**: A series of data points tracking a particular metric over time.

- **Topology**: Map of the M3DB cluster's shard replicas to nodes. Each M3DB cluster has only one placement.
`Placement` and `Topology` are interchangeable terms.
