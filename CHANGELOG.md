# Changelog

# 0.11.1 (pending)

## Features

- **m3aggregator:** Make YAML serialization roundtrip for config related types (#1864)

# 0.11.0

## Migration Disclaimer

Version 0.11.0 of M3 includes further work on supporting writing data at arbitrary times (within the retention period). While most of these changes are transparent to the user in terms of functionality and performance, we had to make a change to the naming format of the files that get persisted to disk (#1720). This change was required to handle multiple fileset volumes per block, which is necessary after introducing "cold flushes" (#1624).

The redesign is **backwards compatible** but not **forwards compatible**. This means that you should be able upgrade your < 0.11.0 clusters to 0.11.0 with no issues, but you will not be able to downgrade without taking some additional steps.

### Troubleshooting and Rolling Back

If you run into any issues with the upgrade or need to downgrade to a previous version for any reason, follow these steps:

1. Stop the node that is having trouble with the upgrade or that you're trying to downgrade.
2. Remove the filesets that include a volume index in them, e.g. this is a filename with the new volumed format: `fileset-1257890400000000000-0-data.db`, and this is the corresponding filename in the original format: `fileset-1257890400000000000-data.db`.
3. Modify the `bootstrappers` config in the M3DB YAML file from `filesystem, commitlog, peers, uninitialized_topology` to `filesystem, peers, commitlog, uninitialized_topology`. This will force the node to bootstrap from its peers instead of the local snapshot and commitlog files it has on disk, which is important otherwise when the node restarts, it will think that it has already been bootstrapped.
4. Turn the node back on.

## Features

- **M3DB**: Add cluster repair capability for non-indexed data (#1831, #1849, #1862, #1874)
- **M3DB**: Enable arbitrary out of order writes for non-indexed data (#1780, #1790, #1829)
- **M3DB**: Expose debug dump on /debug/dump for zip file of profiles and data about the environment for debugging (#1811)
- **M3Query**: Allow lookback duration to be set in query parameters for configurable lookback windowing (#1793)

## Performance

- **M3DB**: Improve base line write performance using pre-encoded tags instead of encoding just before commit log write (#1898, #1904)
- **M3DB**: Avoid allocating expensive digest readers in seek.Open() (#1835)

## Bug Fixes

- **M3Query**: Fix PromQL variadic functions with no params specified (#1846)
- **M3Query**: Fix PromQL absent function (#1871)
- **M3Query**: Fix PromQL binary comparisons that use the time function (#1888)
- **M3Query**: Fix multi-deployment queries (e.g. cross region) using temporal functions (#1901)
- **M3DB**: Index queries to local node now returns only IDs owned by the node at time of read (#1822)
- **M3DB**: Automatically create folder for KV cache, avoiding common error logs (#1757)
- **M3Query**: Batch remote server responses to avoid large individual RPC messages (#1784)
- **M3DB**: Use rlimit and setcap together to raise file descriptor limits to desired setting if possible (#1792, #1745, #1850)

## Documentation

- **M3DB**: Add shard and recommended block sizes (#1890)
- **M3Coordinator**: Improve namespace specific read/write header documentation (#1884)

# 0.10.2

## Performance

- **M3DB**: First of a set of incremental bootstrap improvements, improve commit log reader speed (#1724)

## Bug Fixes

- **M3Query**: Add verbose step size parsing errors to identify problematic queries (#1734)
- **M3Coordinator**: Add verbose errors to identify metrics arriving too late for aggregation and make lateness policies able to be set by config (#1731)

# 0.10.1

## Bug Fixes

- **M3Coordinator**: Use table based approach for aggregation tile buffer past calculation with good defaults for Prometheus remote write latency (#1717)

# 0.10.0

## Features

- **M3Query**: Add multi-zone and multi-region configuration for coordinator (#1687)
- **M3Query**: Add debug param to `GET` `/api/v1/namespace` endpoint for better readability (#1698)
- **M3Coordinator**: Add "ingest_latency" histogram metric and return datapoint too old/new errors with offending timestamps (#1716)

## Performance

- **M3DB**: Add forward index write capability which eases index pressure at block boundaries (#1613)
- **M3Query**: Drop empty series from appearing in output when `keepNaNs` option is set to disabled (#1682, #1684)
- **M3DB**: Build and release M3DB with Go 1.12 (#1674)

## Bug Fixes

- **M3DB**: Fix a bug where peer bootstrapping would sometimes get stuck for a short period of time due to other M3DB nodes (temporarily) returning more than one block of data for a given time period (#1707)
- **M3DB**: Fix a bug that would cause multiple orders of magnitude slow-down in peer bootstrapping / node replacements when one of the nodes in the cluster was hard down (#1677)
- **M3Query**: Fix a bug with parsing Graphite find queries by adding `MatchField` and `MatchNotField` match types, and explicitly making use of them in graphite queries (#1676)
- **M3Query**: Propagate limit settings when using Prometheus Remote Read (#1685)
- **M3Coordinator**: Return a 404 rather than a 500 if we attempt to delete a nonexistent placement (#1701)
- **M3Coordinator**: Return HTTP 400 if all sent samples encounter too old/new or other bad request error (#1692)

# 0.9.6

## Bug Fixes

- **M3DB**: Revert Docker setcap change to allow image to run with no special flags (#1672)

# 0.9.5

## Performance

- **M3DB**: Use ReadAt() pread syscall to reduce syscalls per time series data read and half the number of open FDs with default configuration (#1664)

## Bug Fixes

- **M3Query**: Add missing support for PromQL unary expressions (#1647)
- **M3DB**: Call setrlimit to set open FDs hard limit for docker image and in Dockerfile use setcap to enable capability (#1666)
- **M3DB**: Updated Tally to 3.3.10 to issue correct error message when listen address conflict occurs with Prometheus reporter (#1660)

# 0.9.4

## Bug Fixes

- **M3Query**: Adding complete tags query fanout support with integration test (#1656)

# 0.9.3

## Bug Fixes

- **M3Query**: Add docker integration test for query fanout and fix query aggregating results (#1652)
- **M3DB**: Restore process stats disabled by the open FD counting performance fix (#1648)

# 0.9.2

## Performance

- **M3Query**: Add default and config to limit the number of unique time series fetch from a single DB node to fulfill a query (default 10,000) (#1644)
- **M3Query**: Fix and throttle file descriptor stat emission excessive allocation, also disable Prometheus process collector which had the same issue (#1633)

## Bug Fixes

- **M3DB**: Fix ReturnUnfulfilledForCorruptCommitLogFiles not propagating to bootstrapper options, causing peer bootstrapper to begin when it should not (#1639)
- **M3Query**: Fix PromReadInstantHandler not correctly responding with error message when query encounters error (#1611)

# 0.9.1

## Performance

- **M3DB**: Use index aggregate query to speed up the [/label/<label_name>/values endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values) (#1628)

## Bug Fixes

- **M3Query**: Fixed an issue causing untyped step sizes to fail parsing, now defaults to using seconds as the time unit (#1617)
- **M3Query**: Accept both GET and POST for series match endpoint (#1606)

# 0.9.0

## Features

- **M3Coordinator**: Emit caller I.P address in error logs (#1583)
- **M3Query**: Add a list tags endpoint that displays every unique tag name stored in M3DB to match the Prometheus [/labels endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names) (#1565)
- **M3DB**: Add native support for running M3DB in "index-only" mode (#1596)
- **M3DB**: Reduce M3DB log spam for normal operations like cleanup (#1592)

## Bug Fixes

- **M3DB**: Filter out commit log files that were not present on disk before the process started when determining if files are corrupt in the commitlog bootstrapper. This will prevent a race in which the commitlog bootstrapper would interpret the current commitlog file as corrupt (#1581)
- **M3Query**: Fixed binary arithmetic to properly ignore names when checking for matching series (#1604)
- **M3Query**: Add start and end time to tag values endpoint to fix an issue preventing Grafana tag completion (#1601)

## Performance

- **M3DB**: Optimize some types aggregation queries in M3DB to not require materializing intermediary documents. This should substantially improve query performance for certain types of aggregation queries such as those used to perform auto-complete in Prometheus and Grafana (#1545)
- **M3DB**: Open new commitlog files asynchronously. This should prevent large spikes in the commitlog queue during snapshotting / commitlog rotation for write-heavy workloads (#1576)

# 0.8.4 (2019-04-20)

## Performance

- **M3DB**: Fix index query and aggregate query pooling using finalizers (#1567)

## Bug Fixes

- **M3Query**: Fix PromQL `offset` modifier implementation (#1561)
- **M3DB**: Fix index flush recovery when previous index flush attempts have failed (#1574)

# 0.8.3 (2019-04-12)

## Performance

- **M3DB**: Bump TagLiteralLength limit to 64K (#1552)

# 0.8.2 (2019-04-11)

## New Features

- **M3DB**: Add Jaeger tracing to M3DB QueryIDs path (#1506)

## Performance

- **M3DB**: Add postingsList caching for FieldQuery (#1530)
- **M3DB**: Add write & read latency histogram metrics to M3DB client and emit from M3Coordinator (#1533)
- **M3DB**: Persist block at retention edge when building index after peer streaming (#1531)

## Bug Fixes

- **M3Query**: Fixed a bug that could cause a panic when converting queries (#1546)

# 0.8.1 (2019-04-02)

## Bug Fixes

- **M3DB**: Fixed a bug that would sometimes prevent successfully retrieved time series data from being loaded into the cache.
- **M3DB**: Fixed a bug where an error was not being properly logged.

# 0.8.0 (2019-03-29)

## Migration Disclaimer

Version 0.8.0 of M3 switches M3DB to use file descriptors and the Read() system call for reading time series data files instead of mmaps.
This should improve read latency and memory utilization for some workloads, particularly those in which the amount of data on disk vastly exceeds the amount of memory on the system.
This changes also enables the ability to increase the fetch concurrency past the default value.

As a result of this change, M3DB will allocate significantly less mmaps, but will create a corresponding amount of file descriptors.

Operators may need to tune their kernel configuration to allow a higher number of open file descriptors. Please follow our [Kernel Configuration Guide](http://m3db.github.io/m3/operational_guide/kernel_configuration/) for more details.

## New Features

- **M3DB**: Add dedicated aggregation endpoint (#1463, #1502, #1483)

## Performance

- **M3DB**: Use Seek() and Read() APIs for index/data files instead of mmap + share expensive seeker resources among seekers (#1421)
- **M3Query**: Tag completion endpoints (Prometheus (`match[]`, `/{foo}/series`), and Graphite (`find`) endpoints) now use the new dedicated aggregation endpoint (#1481)

# 0.7.3 (2019-03-22)

## New Features

- **M3DB**: Add an AllQuery index query type (#1478)

## Bug Fixes

- **M3DB**: Fix to annotation pooling (#1476)
- **M3Coordinator**: Only panics log a stacktrace rather than expected errors (#1480)

## Performance

- **M3DB**: Use a single results object for merging postings lists across blocks, rather than creating a result per block and discarding them immediately thereafter. This should reduce memory utilization significantly for queries that look back over a large number of blocks (#1474)
- **M3DB**: Improvement to applying back pressure on writes with a full commitlog, which should improve M3DB's ability to apply back pressure under load and recover quickly after being overwhelmed with traffic. (#1482)

# 0.7.2 (2019-03-15)

## Bug Fixes

- **All Binaries**: Fix LD flags in release so that version, branch, build date, etc are packaged into the binary properly.

# 0.7.1 (2019-03-15)

## New Features

- **M3Query**: Add per-query cost accounting to allow fine-grained controls on datapoint limits to Prometheus and Graphite style queries (#1207) (#1449)
- **M3Query**: Add optional pickled return type for Graphite render endpoint (#1446)
- **M3Query**: Drop NaNs from query results (#1458)

## Bug Fixes

- **M3DB**: Fix bug in postings list caching which could cause incorrect results on queries (#1461)

# 0.7.0 (2019-03-12)

## Migration Disclaimer

Version 0.7.0 of M3 includes a redesign of the snapshotting and commitlog components (#1384 among others). This redesign was critical to improve M3DB's consistency guarantees, reduce the amount of disk space that is wasted by commitlogs, and enable future feature development to support writing data at arbitrary times.

The redesign is **backwards compatible** but not **forwards compatible**. This means that you should be able upgrade your < 0.7.0 clusters to 0.7.0 with no issues, but you will not be able to downgrade without taking some additional steps. Note that the first bootstrap after the upgrade may take longer than usual, but subsequent bootstraps should be just as fast as they used to be, or even faster.

### Troubleshooting and Rolling Back

If you run into any issues with the upgrade or need to downgrade to a previous version for any reason, follow these steps:

1. Stop the node that is having trouble with the upgrade or that you're trying to downgrade.
2. Modify the `bootstrappers` config in the M3DB YAML file from `filesystem, commitlog, peers, uninitialized_topology` to `filesystem, peers, commitlog, uninitialized_topology`. This will force the node to bootstrap from its peers instead of the local snapshot and commitlog files it has on disk, bypassing any issues related to file incompatibility between versions.
3. Turn the node back on and wait for it to finish bootstrapping and snapshotting. Once everything looks stable, change the config back to `filesystem, commitlog, peers, uninitialized_topology` so that the next time the node is restarted it will default to using the snapshot and commitlog files.

## New Features

- **M3DB**: Restructuring of commitlog and snapshotting feature as described above (#1384)
- **M3DB**: Obtain a lock on data directory on startup (#1376)
- **M3Coordinator**: Add support for zone / environment override headers in namespace APIs so multiple M3DB clusters can be administered from the same M3Coordinator instance / etcd cluster (#1427)
- **M3Query**: Add Jaeger tracing to M3Query, and mechanism to plugin custom opentracing implementations (#1321)
- **M3nsch**: Add basic Grafana dashboard showing success rates and latencies (#1401)
- **M3nsch**: Support generating new unique metrics (#1397)

## Performance

- **M3DB**: Optimize OStream implementation which reduces CPU synchronization for each write. Should result in modest improvement in load average for metrics workloads and a massive improvement in load average for any workload using large annotations (#1399, #1437)
- **M3DB**: Prevent duplicate writes from being written to the commitlog (#1375)
- **M3DB**: Construct RPC service once and share it with TChannel and HTTP servers to prevent pools from being initialized multiple times reducing memory footprint (#1420)
- **M3Query**: Add LRU cache for query conversion. Should help dashboards with expensive regex query in particular (#1398)

## Bug Fixes

- **M3Coordinator**: Better error responses from namespace APIs when namespace is unknown (#1412)
- **M3Query**: Fix panic in temporal functions (#1429)

# 0.6.1 (2019-02-20)

## Bug fixes

- **M3Coordinator**: Fix to panic caused by generating ID for series with no tags (#1392)
- **M3Coordinator**: Fix to panic caused by reading placement when none available instead of return 404 (#1391)

# 0.6.0 (2019-02-19)

## Breaking changes

- **M3Coordinator**: ID generation scheme must be explicitly defined in configs ([Set "legacy" if unsure, further information on migrating to 0.6.0](http://m3db.github.io/m3/how_to/query/#migration)) (#1381)

## New Features

- **M3DB** (Config): Simplify M3 config options (#1371)
- **M3Coordinator**: Improve database creation API (#1350)
- **M3Query**: Add quantile_over_time and histogram_quantile Prometheus functions (#1367, #1373)
- **Documentation**: Additional documentation for namespace setup and configuration, etcd, and M3Coordinator ID generations schemes (#1350, #1354, #1381, #1385)

## Performance

- **M3DB** (Index): Add posting list cache that should result in a massive improvement for M3DB query performance for most workloads (#1370)

## Bug fixes

- **M3DB** (Index): Fix race condition in index query (#1356)
- **M3Coordinator**: Fix panic responder which was previously not reporting query panics to users (#1353)
- **M3Query**: Fix bug in calculating temporal (over_time) functions (#1271)

# 0.5.0 (2019-02-03)

## New Features

- **M3Coordinator**: Add [Graphite support](http://m3db.github.io/m3/integrations/grafana/) in the form of Carbon ingestion (with configurable aggregation and storage policies), as well as direct and Grafana based Graphite querying support (#1309, #1310, #1308, #1319, #1318, #1327, #1328)
- **M3Coordinator**: Add tag completion API (#1175)
- **M3Coordinator**: Add new opt-in ID generation function that will never collide (#1286)
- **M3DB**: Add [endpoint](https://m3db.io/openapi/#operation/databaseConfigSetBootstrappers) for setting database bootstrapers dynamically(#1239)

## Performance

- **M3DB** (Index) Replace usage of slow "memory segment" for index segment with immutable F.S.Ts that are constantly being generated in the foreground as series are being inserted. Significantly reduces query latency (10x+) for some types of workloads that make heavy use of regex (#1197)
- **M3DB**: (Index) Add support for concurrent index block queries (improves performance of queries that look back many blocks (#1195)
- **M3DB**: (Index) Improve pooling configuration of one of the index array pools to prevent (almost) unbounded growth over time for certain workloads (#1254)
- **M3DB**: (Index) Use only one roaring bitmap library (Pilosa), and upgrade to version of Pilosa with our upstreammed `UnionInPlace` improvements that reduces memory consumption of expensive queries by over a factor of 10x (#1238), fixes: #1192
- **M3DB**: (Index) Don't use object pool for allocating long-lived arrays of tag slices which reduces steady-state memory consumption because the default size is 16 which is much bigger than the number of tags most metrics have (#1300)
- **M3DB**: Auto-calculate size of WriteBatchPool based on commitlog queue size and improve chance of batch being returned to pool (#1236)
- **M3DB**: Don't allow msgpack library to allocate \*bufio.Reader (reduces allocations) and mmap bloomfilters and index summaries as files instead of anonymously so they can be paged out by the O.S if necessary (#1289)

## Bug fixes

- **M3DB**: Fix bug where namespace-level configuration "writes to commitlog" was not respected (#1232)
- **M3DB**: Improve how M3DB handles durability during topology changes. Previously, a new node that was added to an M3DB cluster would not be completely durable (able to recover all data from disk) for a few minutes after the node join completed even though it marked all its shards as available. Now, newly added M3DB nodes will never mark their shards as available until they are completely durable (#1183). Also, Make M3DB health check not return success until node is bootstrapped AND durable, not just bootstrapped. This makes automated operations (like those performed by the Kubernetes operator or various scripts) much safer (#1287)
- **M3DB**: Make it possible to safely replace/add/remove M3DB seed nodes by exposing etcd configuration (#1339)
- **M3Coordinator**: Fix bug in database create API so it respects number of shards (#1188)
- **M3Coordinator**: Fix tag propagation for temporal functions (#1307)
- **M3Coordinator**: Properly propagate M3DB fetch timeout from config instead of using default value (#1342)

# 0.4.8 (2018-10-20)

- **Coordinator**: Reduce log spam for high latency requests (#1164)
- **Coordinator**: Add support for replace API for db nodes (#1162)
- **Coordinator**: Add sort and sort_desc and linear regression promql functions (#1104, #1063)
- **DB**: Emit logs when process limits are misconfigured (#1118)
- **DB**: Change the implementation of the snapshotting process to snapshot all unflushed blocks (#1017)
- **DB**: Remove CacheAllMetadata policy (#1110)
- FIX **Coordinator**: Fixed bug in database creation API: error not displayed in logs (#1089)
- FIX **DB**: Various changes that improve M3DBs resiliency to corrupt commitlog files during bootstrap and cleanup as well as its resiliency to corrupt fileset files (#1065, #1066, #1086)
- FIX **Coordinator**: Fix OpenAPI yml, was previously broken and would not render (#1062)
- PERF **DB**: Sample M3DB write method timers to improve performance for high write throughput workloads (#1057)
- PERF **DB**: Massive improvement in write throughput (2-3 in some cases) by improving the speed of the commitlog msgpack encoder and removing contention on the commitlog queue via batching (#1160, #1157)

# 0.4.7 (2018-10-10)

- **Aggregator, Collector**: Add m3aggregator and m3collector for clustered downsampling (440b41657, df3999d58, #1030, #1038, #1050, #1061)
- **Coordinator**: Add m3msg server and placement and topic APIs in m3coordinator to enable use as backend with m3aggregator (#1028, #1055, #1060)
- DOCS **DB**: Add doc links to placement and namespace config operational guides (#1029)

# 0.4.6 (2018-10-05)

- **Coordinator**: Add cluster namespace fanout heuristics supporting queries greater than retention (#908)
- **Coordinator**: Add ability for query storage to provide unconsolidated blocks (#929)
- FIX **Coordinator**: Multi-fetch fixes (#989)
- FIX **Coordinator**: Disable CGO on linux builds (#969)
- FIX **Coordinator**: Write fanouts with aggregated namespaces (#991)
- PERF **Coordinator**: Set namespace and ID as NoFinalize (#956)
- PERF **Coordinator**: Treat Prometheus TagName/Value as []byte instead of String (#1004)
- PERF **Coordinator**: Improve performance of generatings IDs from tags in M3Coordinator (#1000)
- PERF **Coordinator**: Significantly improve performance of FetchResultToPromResult and helper functions (#1003)
- PERF **Coordinator**: Improve M3DB session performance part 2: Dont clone IDs if they are IsNoFinalize() (#986)
- PERF **Coordinator**: Improve M3DB session performance part 1: Pool goroutines in host queues (#985)
- PERF **Coordinator**: Use pooling for writes to coordinator (#942)
- PERF **Coordinator**: Use a better pool size for coordinator writes (#976)
- PERF **Coordinator**: Avoid duplicate ident.ID conversions (#935)
- DOCS **DB**: Add placement change operational guide (#998)
- DOCS **DB**: Add namespace modification operational guide (#995)
- DOCS **DB**: [integrations] systemd: add config flag to service (#974)
- DOCS **DB**: Add operational guides for M3DB (topology and bootstrapping) (#924)
- DOCS **DB**: [integrations] systemd: add systemd unit example (#970)
- DOCS **DB**: Update bootstrapping operational guide (#967)
- DOCS **DB**: Timeout topology watch and return better error messages for missing topology / namespaces (#926)
- DOCS **DB**: Add Prom/Grafana dashboard for M3DB as well as docker-compose for local development of m3-stack (#939)

# 0.4.5 (2018-09-24)

- FIX **DB** Index data race in FST Segment reads (#938)

# 0.4.4 (2018-09-21)

- **DB:** Use commit log bootstrapper before peers bootstrapper (#894)
- **Query:** Add more PromQL functions (irate, temporal functions, etc) (#872, #897)
- **Coordinator:** Add downsampling to all configured namespaces by default (#890)
- **DB:** By default use upsert behavior for datapoints written to mutable series (#876)
- PERF **DB:** Various performance updates (#889, #903)

# 0.4.3 (2018-09-05)

- **Query:** Make compatible with Grafana Prometheus data source (#871, #877)
- FIX **DB:** Fix index correctness for multi-segment reads (#883)
- PERF **DB:** Index performance improvements (#880)

# 0.4.2 (2018-08-30)

- FIX **DB:** Remove native pooling and remove possibility of it being used by any components (#870)
- FIX **DB:** Fix LRU series cache locking during very high throughput (#862)

# 0.4.1 (2018-08-27)

- FIX **Coordinator:** Add support for negated match and regexp Prometheus remote read queries (#858)

# 0.4.0 (2018-08-09)

- **Coordinator:** Add downsampling capabilities (#796)
- **Query:** Add dedicated m3query service for serving PromQL and other supported query languages (#817)
- **DB:** Add commit log snapshotting support to significantly reduce disk space usage by removing the majority of uncompressed commit logs on a database node and speed up bootstrap time (#757) (#802)

- **Coordinator:** Add downsampling capabilities (#796)
- **Query:** Add dedicated m3query service for serving PromQL and other supported query languages (#817)
- **DB:** Add commit log snapshotting support to significantly reduce disk space usage by removing the majority of uncompressed commit logs on a database node and speed up bootstrap time (#757) (#802)

# 0.3.0 (2018-07-19)

- **Coordinator:** Add m3coordinator Dockerfile and update Prometheus test to use standalone coordinator (#792)
- **Coordinator:** Add rudimentary multi-cluster support for m3coordinator (#785)
- **DB:** Fix documentation for single node walkthrough (#795)

# 0.2.0 (2018-05-26)

- **Coordinator:** Build dedicated m3coordinator binary for release binaries

# 0.1.0 (2018-05-24)

- **DB:** Reverse indexing
- **Coordinator:** Prometheus remote read/write support
