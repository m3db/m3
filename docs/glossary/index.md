# Glossary

- **Bootstrapping**: Process by which an M3DB node is brought up. Bootstrapping consists of determining the integrity of data that the node has, replay writes from the commit log, and/or stream missing data from its peers.

- **M3**: Highly scalable, distributed metrics platform that is comprised of a native, distributed time series database, a highly-dynamic and performant aggregation service, a query engine, and other supporting infrastructure.

- **M3DB**: Distributed time series database influenced by [Gorilla](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) and [Cassandra](http://cassandra.apache.org/) released as open source by Uber Technologies.

- **Namespace**: Similar to a table in other types of databases, namespaces in M3DB have a unique name and a set of configuration options, such as data retention and block size.

- **Placement**: Map of the M3DB cluster's shard replicas to nodes. Each M3DB cluster has only one placement.

- **Shard**: Effectively the same as a "virtual shard" in Cassandra in that it provides an arbitrary distribution of time series data via a simple hash of the series ID.

- **Topology**: See *Placement*
