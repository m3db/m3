# Changelog

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
