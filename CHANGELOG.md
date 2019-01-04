Changelog
=========
# (unreleased)

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

- **Aggregator, Collector**:  Add m3aggregator and m3collector for clustered downsampling (440b41657, df3999d58, #1030, #1038, #1050, #1061)
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
