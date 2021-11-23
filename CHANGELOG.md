# Changelog

# 1.4.1

## Bug Fixes
- **M3Coordinator**: Do not Close singleton MessageProcessors when closing connections. This fixes a panic introduced that affects M3Coordinator -> M3Aggregator communication. ([#3934](https://github.com/m3db/m3/pull/3934))

# 1.4.0

## Features
- **M3Query**: Add write endpoint support for M3-Map-Tags-JSON header in InfluxDB path ([#3816](https://github.com/m3db/m3/pull/3816))
- **M3Query**: Add support for `last_over_time` in M3Query engine ([#3884](https://github.com/m3db/m3/pull/3884))
- **M3Aggregator**: Add p75/p25 as aggregation options ([#3867](https://github.com/m3db/m3/pull/3867))

## Bug Fixes
- **M3DB**: Fix M3TSZ to be deterministic when encoding high precision values ([#3872](https://github.com/m3db/m3/pull/3872))
- **M3DB**: Gracefully handle reads including documents with stale index state ([#3905](https://github.com/m3db/m3/pull/3905))

## Performance
- **M3Aggregator**: Rework close and remove `persitFlushTimesEvery` semantics in leader flushing in favour of always persisting shard flush times on a successful flush for optimized graceful failovers ([#3890](https://github.com/m3db/m3/pull/3890))
- **M3DB**: Optimize `filesetFiles` function during bootstrapping for namespaces with long retentions to prevent CPU spikes ([#3900](https://github.com/m3db/m3/pull/3900))
- **M3DB**: Avoid loading blocks in memory for namespaces with snapshots disabled during bootstrapping to reduce memory usage ([#3919](https://github.com/m3db/m3/pull/3919))

# 1.3.0

## Features

- **M3Coordinator**: Add support for Prometheus Remote Write storage backend for sending aggregated and unaggregated metrics ([#3742](https://github.com/m3db/m3/pull/3742), [#3768](https://github.com/m3db/m3/pull/3768), [#3783](https://github.com/m3db/m3/pull/3783), [#3791](https://github.com/m3db/m3/pull/3791), [#3814](https://github.com/m3db/m3/pull/3814), [#3777](https://github.com/m3db/m3/pull/3777))
- **M3Coordinator**: Add support for InfluxDB write endpoint GZip compression, setting timestamp precision and allowing an empty request body ([#3373](https://github.com/m3db/m3/pull/3373))
- **M3DB**: Add SYSCTL_VM_MAX_MAP_COUNT env var for sysctl-setter sidecar allowing for custom VM max map count ([#3689](https://github.com/m3db/m3/pull/3689))

## Bug Fixes

- **M3DB**: Fix writes briefly degrading when creating a new namespace due to coarse lock acquisition ([#3765](https://github.com/m3db/m3/pull/3765))
- **M3DB**: Fix compiled regexp DFA cache eviction on full bug that can lead to slow memory leak with large number of unique regexps ([#3806](https://github.com/m3db/m3/pull/3806))

## Performance

- **M3Coordinator**: Update default M3Msg retry initial backoff from 1s to 5s to reduces timeout and retries in large clusters ([#3820](https://github.com/m3db/m3/pull/3820))
- **M3DB**: Fix performance of reverse index queries that cover huge time ranges ([#3813](https://github.com/m3db/m3/pull/3813))

# 1.2.0

## Features

- **M3Query**: Support Prometheus matchers with match[] URL parameters in label endpoints ([#3180](https://github.com/m3db/m3/pull/3180))
- **M3Query**: Support Prometheus start and end time URL parameters for label and series metadata endpoints ([#3214](https://github.com/m3db/m3/pull/3214))
- **M3Query**: Add Graphite functions and update functions with new arguments that were missing ([#3048](https://github.com/m3db/m3/pull/3048), [#3367](https://github.com/m3db/m3/pull/3367), [#3370](https://github.com/m3db/m3/pull/3370), [#3145](https://github.com/m3db/m3/pull/3145), [#3149](https://github.com/m3db/m3/pull/3149), [#3142](https://github.com/m3db/m3/pull/3142), [#3469](https://github.com/m3db/m3/pull/3469), [#3484](https://github.com/m3db/m3/pull/3484), [#3545](https://github.com/m3db/m3/pull/3545), [#3576](https://github.com/m3db/m3/pull/3576), [#3582](https://github.com/m3db/m3/pull/3582), [#3583](https://github.com/m3db/m3/pull/3583), [#3521](https://github.com/m3db/m3/pull/3521), [#3602](https://github.com/m3db/m3/pull/3602), [#3641](https://github.com/m3db/m3/pull/3641), [#3644](https://github.com/m3db/m3/pull/3644), [#3648](https://github.com/m3db/m3/pull/3648))
- **M3Query**: Fix Graphite treatment of `**` to allow to match an empty segment instead of one or more ([#3366](https://github.com/m3db/m3/pull/3366), [#3593](https://github.com/m3db/m3/pull/3593))
- **M3Query**: Add M3-Limit-Max-Range header to optionally truncate time range of queries ([#3538](https://github.com/m3db/m3/pull/3538))
- **M3Coordinator**: Add ability to use an exclude by rollup rule to rollup metrics without specific dimensions ([#3318](https://github.com/m3db/m3/pull/3318))
- **M3DB**: Use better heuristics to cap the series and aggregate query limits that individual DB nodes apply for a query so in larger clusters the query can be clamped earlier ([#3516](https://github.com/m3db/m3/pull/3516), [#3518](https://github.com/m3db/m3/pull/3518), [#3519](https://github.com/m3db/m3/pull/3519), [#3520](https://github.com/m3db/m3/pull/3520), [#3527](https://github.com/m3db/m3/pull/3527))
- **M3DB**: Add repair option full_sweep and ability to force a repair via API call ([#3573](https://github.com/m3db/m3/pull/3573), [#3550](https://github.com/m3db/m3/pull/3550))

## Bug Fixes

- **M3DB**: Fix aggregate series metadata query limits ([#3112](https://github.com/m3db/m3/pull/3112))
- **M3Coordinator**: Make bad aggregated namespace headers return bad request status code instead of internal server error ([#3070](https://github.com/m3db/m3/pull/3070))
- **M3Coordinator**: Propagate Require-Exhaustive parameter for aggregate series metadata queries ([#3115](https://github.com/m3db/m3/pull/3115))
- **M3Query**: Add determinism to Graphite sort and reduce functions ([#3164](https://github.com/m3db/m3/pull/3164))

## Performance

- **M3DB**: Rearchitect index segments to compact and expire series on block rotation instead of build a new segment for new block ([#3464](https://github.com/m3db/m3/pull/3464))
- **M3DB**: Add postings list cache for searches and repopulate during active block index segment compaction before segment made visible for queries ([#3671](https://github.com/m3db/m3/pull/3671))
- **M3DB**: Avoid allocating index entry fields per series and read from backing mmap directly ([#3050](https://github.com/m3db/m3/pull/3050), [#3062](https://github.com/m3db/m3/pull/3062), [#3057](https://github.com/m3db/m3/pull/3057))
- **M3DB**: Avoid allocating series IDs when read from disk ([#3093](https://github.com/m3db/m3/pull/3093))
- **M3DB**: Improve speed of tag byte reuse from ID for tags by speeding up search ([#3075](https://github.com/m3db/m3/pull/3075))
- **M3DB**: Improve speed of bootstrap by using StreamingReadMetadata API for reads from disk ([#2938](https://github.com/m3db/m3/pull/2938))
- **M3DB**: Improve speed of bootstrap by using an asynchronously evaluated series resolver API that can be written to while bootstrapping reliably ([#3316](https://github.com/m3db/m3/pull/3316))
- **M3DB**: Add limits for total series being read at any one time globally ([#3141](https://github.com/m3db/m3/pull/3141))
- **M3DB**: Restrict the time a query can hold an index worker to help allow small queries to continue to execute while larger ones are paused and resumed ([#3269](https://github.com/m3db/m3/pull/3269))
- **M3DB**: Use adaptive WriteBatch allocations to dynamically match workload throughput and batch sizes ([#3429](https://github.com/m3db/m3/pull/3429))
- **M3Coordinator**: Improve rule matching speed by improving per element rule matching and disabling cache which puts locks in the hot path ([#3080](https://github.com/m3db/m3/pull/3080), [#3083](https://github.com/m3db/m3/pull/3083))
- **M3Query**: Improve speed of M3TSZ decoding by using 64 bit operations ([#2827](https://github.com/m3db/m3/pull/2827))
- **M3Query**: Improve speed of M3TSZ decoding by using int64 type xtime.UnixNano instead of time.Time ([#3515](https://github.com/m3db/m3/pull/3515))
- **M3Query**: Improve speed of quorum reads by improving multi-replica iterator ([#3512](https://github.com/m3db/m3/pull/3512))

# 1.1.0

## Features

- **M3Coordinator**: Add /ready endpoint for readiness probe which checks current write/read consistency level achievability ([#2976](https://github.com/m3db/m3/pull/2976))
- **M3Coordinator**: Add per endpoint status code response codes and latency metrics ([#2880](https://github.com/m3db/m3/pull/2880))
- **M3Coordinator**: Add Graphite carbon ingest latency metrics ([#3045](https://github.com/m3db/m3/pull/3045))
- **M3Coordinator**: Add Graphite carbon ingest rule matcher contains to compliment regexp for faster matching ([#3046](https://github.com/m3db/m3/pull/3046))
- **M3Coordinator**: Return 504 errors on timeout to downstream M3DB nodes or other cross-region coordinators ([#2886](https://github.com/m3db/m3/pull/2886))
- **M3Coordinator**: Validate placements when using the raw placement upsert endpoint unless force set is specified ([#2922](https://github.com/m3db/m3/pull/2922))
- **M3Query**: Add Graphite powSeries function ([#3038](https://github.com/m3db/m3/pull/3038))
- **M3Query**: Add Graphite support for ** with metric path selectors ([#3020](https://github.com/m3db/m3/pull/3020))
- **M3DB**: Add ability to configure regexp DFA and FSA limits ([#2926](https://github.com/m3db/m3/pull/2926))
- **M3DB**: Add Alibaba Cloud storage class Kubernetes manifest for disk provisioning in Aliyun ([#2908](https://github.com/m3db/m3/pull/2908))

## Bug Fixes

- **M3Coordinator**: Always set content type JSON for error responses ([#2917](https://github.com/m3db/m3/pull/2917))
- **M3Query**: Fix invalid query resulting in 500 instead of 400 ([#2910](https://github.com/m3db/m3/pull/2910))
- **M3Query**: Allow Graphite variadic functions to omit variadic args ([#2882](https://github.com/m3db/m3/pull/2882))

## Performance

- **M3DB**: Skip out of retention index segments during bootstrap ([#2992](https://github.com/m3db/m3/pull/2992))

## Documentation

- **All**: Add clustering getting started guides for both Kubernetes operator and binaries deployment ([#2795](https://github.com/m3db/m3/pull/2795))

# 1.0.0

## Overview

This release makes breaking changes to the APIs and configuration to provide a simpler experience both for setup and operating M3.

- New [M3 website](https://m3db.io/).
- New [M3 documentation](https://m3db.io/docs).
- Simple [M3DB configuration](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd.yml) and [guides](https://m3db.io/docs/quickstart/docker/).
- M3DB [hard limits](https://m3db.io/docs/operational_guide/resource_limits/) limits for high resiliency under load.
- Bootstrap rearchitecture, now able to boostrap hundreds of millions of recently written datapoints in minutes for reads on restart.
- Continued focus on baseline performance release-over-release.

## Features
- **M3DB**: Namespace resolution and retention now configured dynamically via API and stored in etcd instead of being defined statically in M3Coordinator configuration.
```
message DatabaseCreateRequest {
  // ...

  // Optional aggregated namespace to create in 
  // addition to unaggregated namespace
  AggregatedNamespace aggregated_namespace = 8;
}
```
- **M3DB**: Minimal configuration file with default settings looks like:
```
coordinator: {}
db: {}
```
and includes common settings such as global query limits.

## Backwards Incompatible Changes

### Configuration
- **M3DB**: `db.bootstrap.bootstrappers` removed
- **M3DB**: `db.config` nested under `db.discovery.config` (`discovery` can optionally accept different `type`s of defaults instead of a custom `config`)
- **M3Coordinator**: `cluster.namespaces.storageMetricsType` removed
- **M3Coordinator**: `tagOptions.tagOptions` no longer supports `legacy` type
- **M3Query**: `limits.perQuery.maxComputedDatapoints` removed
- **M3Query**: `limits.perQuery.maxFetchedDatapoints` removed
- **M3Query**: `limits.global.maxFetchedDatapoints` removed
- **M3Query**: `cache` removed
- **M3Query**: `listenAddress` changed to always be resolved as a string from config. Format changed from
```
listenAddress:
  config: "..."
  value: "..."
```
to 
```
listenAddress: "..."
```

### API
- **M3DB**: `/services/m3db/database/config/bootstrappers` dynamic bootstappers endpoint removed
- **M3Coordinator**: Removed deprecated URL `/api/v1/namespace` in favor of stable preferred URL `/api/v1/services/m3db/namespace`
- **M3Coordinator**: Removed deprecated URL `/api/v1/namespace/init` in favor of stable preferred URL `/api/v1/services/m3db/namespace/init`
- **M3Coordinator**: Removed deprecated URL `/api/v1/namespace/unagg` in favor of stable preferred URL `/api/v1/services/m3db/namespace/unagg`
- **M3Coordinator**: Removed deprecated URL `/api/v1/placement` in favor of stable preferred URL `/api/v1/services/m3db/placement`
- **M3Coordinator**: Removed deprecated URL `/api/v1/placement/init` in favor of stable preferred URL `/api/v1/services/m3db/placement/init`

### Package
- `github.com/m3db/m3/src/x/close` removed in favor of `github.com/m3db/m3/src/x/resource`
- `github.com/m3db/m3/src/dbnode/clock` removed in favor of `github.com/m3db/m3/src/x/clock`
- `github.com/m3db/m3/src/x/dice/dice.go` moved to `github.com/m3db/m3/src/dbnode/storage/dice.go`
- `github.com/m3db/m3/src/x/lockfile/lockfile.go` moved to `github.com/m3db/m3/src/dbnode/server/lockfile.go`

### Misc
- **M3Query**: Concept of data point limit enforcers removed in favor of the other remaining query limits (e.g. max series). This also removed metrics `cost_reporter_datapoints`, `cost_reporter_datapoints_counter`, and `cost_reporter_over_datapoints_limit`.
- Linter enabled

# 0.15.17

## Features 
- **M3Query**: Add aggregate Graphite function ([#2584](https://github.com/m3db/m3/pull/2584))
- **M3Query**: Add applyByNode Graphite function ([#2654](https://github.com/m3db/m3/pull/2654)) 
- **M3Query**: Graphite ParseTime function support greatly expanded to be more in line with Graphite allowances ([#2621](https://github.com/m3db/m3/pull/2621)) 

## Bug Fixes

- **M3Aggregator**: Add default m3msg write timeouts to mitigate deadlocking writes with a stale TCP connection ([#2698](https://github.com/m3db/m3/pull/2698))
- **M3DB**: Fix a bug in bootstrap index caching that would cause long bootstrap times ([#2703](https://github.com/m3db/m3/pull/2703))
- **M3Query**: Fix Graphite constantLine() function to return 3 steps ([#2699](https://github.com/m3db/m3/pull/2699))
- **M3Query**: Fix Graphite limit snapping bug in movingAverage and movingMedian functions ([#2694](https://github.com/m3db/m3/pull/2694))

# 0.15.16

## Features

- **M3Query**: Add divideSeriesLists Graphite function ([#2585](https://github.com/m3db/m3/pull/2585))
- **M3Query**: Add integralByInterval Graphite function ([#2596](https://github.com/m3db/m3/pull/2596))
- **M3Query**: Add highest, lowest Graphite functions ([#2623](https://github.com/m3db/m3/pull/2623))
- **M3Query**: Add resolution exceeds query range warning ([#2429](https://github.com/m3db/m3/pull/2429))

## Documentation

- **M3Coordinator**: Added OpenAPI specification for namespace update endpoint ([#2629](https://github.com/m3db/m3/pull/2629))

## Misc

- **M3Coordinator**: Add config option for writes to leaving shards to count towards consistency and read level unstrict all ([#2687](https://github.com/m3db/m3/pull/2687))
- **All**: Upgrade TChannel to v1.14 ([#2659](https://github.com/m3db/m3/pull/2659))

# 0.15.15

## Features

- **M3DB**: Add configuration to limit bytes read for historical metrics in a given time window ([#2627](https://github.com/m3db/m3/pull/2627))
- **M3DB**: Add configuration to toggle block caching ([#2613](https://github.com/m3db/m3/pull/2613))
- **M3Coordinator**: Add extended configuration of label and tag validation ([#2647](https://github.com/m3db/m3/pull/2647))

## Performance

- **M3DB**: Perform single pass when reading commit log entry and reuse result for second bootstrap phase ([#2645](https://github.com/m3db/m3/pull/2645))

## Documentation

- **M3DB**: Documentation for fileset migrations, the forward and backwards compatibility guarantees and configuring migrations ([#2630](https://github.com/m3db/m3/pull/2630))

# 0.15.14

## Features

- **M3Query**: Add movingSum, movingMax, movingMin Graphite functions ([#2570](https://github.com/m3db/m3/pull/2570))
- **M3Query**: Add aliasByTags Graphite function ([#2626](https://github.com/m3db/m3/pull/2626))
- **M3Query**: Add exponentialMovingAverage Graphite function ([#2622](https://github.com/m3db/m3/pull/2622))
- **M3Query**: Add timeSlice Graphite function ([#2581](https://github.com/m3db/m3/pull/2581))
- **M3Query**: Add delay Graphite function ([#2567](https://github.com/m3db/m3/pull/2567))
- **M3Query**: Add aggregateWithWildcards Graphite function ([#2582](https://github.com/m3db/m3/pull/2582))
- **M3Query**: Add groupByNodes Graphite function ([#2579](https://github.com/m3db/m3/pull/2579))
- **M3Coordinator**: Allow using the placement set API for creating new placements as well as upserting existing placements ([#2625](https://github.com/m3db/m3/pull/2625))
- **M3DB**: Add bootstrap time migration config which supports seamless backwards and forwards compatible fileset upgrades ([#2521](https://github.com/m3db/m3/pull/2521))

# 0.15.13

## Bug Fixes

- **M3DB**: Fix case insensitive regexp modifiers (?i) implementation for metrics selector ([#2564](https://github.com/m3db/m3/pull/2564))

## Misc

- **M3DB**: Expose stream batch client options to config ([#2576](https://github.com/m3db/m3/pull/2576))
- **M3Query**: Metrics visibility and ability to limit number of encoders per block ([#2516](https://github.com/m3db/m3/pull/2516))

# 0.15.12

## Bug Fixes

- **M3Query**: Fix to Graphite movingMedian and movingAverage functions that could skip data in certain cases or cause an out of bounds error after recovery ([#2549](https://github.com/m3db/m3/pull/2549))
- **M3Coordinator**: Fix a Graphite carbon ingest tag lifecycle bug that could cause duplicate tags ([#2549](https://github.com/m3db/m3/pull/2549))

# 0.15.11

## Features

- **M3Coordinator**: Support for remapping rules and provide tags that are auto-appended to metrics when aggregations are applied ([#2414](https://github.com/m3db/m3/pull/2414))

## Bug Fixes

- **M3DB**: Extend lifetime of compactable index segments for aggregate queries ([#2550](https://github.com/m3db/m3/pull/2550))

# 0.15.10

## Features

- **M3DB**: Add migration task for filesets from v1.0 to v1.1 ([#2520](https://github.com/m3db/m3/pull/2520))

## Bug Fixes

- **M3DB**: Fix enqueue readers info file reading ([#2546](https://github.com/m3db/m3/pull/2546))

## Documentation

- **All**: Fix buildkite mkdocs script ([#2538](https://github.com/m3db/m3/pull/2538))

# 0.15.9

## Performance

- **M3DB**: Background cold flush process to no longer block data snapshotting or commit log rotation ([#2508](https://github.com/m3db/m3/pull/2508))
- **M3DB**: Avoid sorting index entries when reading data filesets during bootstrap when not required ([#2533](https://github.com/m3db/m3/pull/2533))

## Bug Fixes

- **M3Coordinator**: Respect M3Cluster headers in namespace GET ([#2518](https://github.com/m3db/m3/pull/#2518))

## Documentation

- **M3Aggregator**: Add M3Aggregator documentation ([#1741](https://github.com/m3db/m3/pull/1741), [#2529](https://github.com/m3db/m3/pull/2529))
- **M3DB**: Bootstrapper documentation fixes ([#2510](https://github.com/m3db/m3/pull/2510))
- **All**: Update mkdocs ([#2524](https://github.com/m3db/m3/pull/2524), [#2527](https://github.com/m3db/m3/pull/2527))
- **All**: Add M3 meetup recordings ([#2495](https://github.com/m3db/m3/pull/2524), [#2527](https://github.com/m3db/m3/pull/2495))
- **All**: Update Twitter link ([#2530](https://github.com/m3db/m3/pull/2530))
- **All**: Fix spelling in FAQ ([#2448](https://github.com/m3db/m3/pull/2448))

## Misc

- **M3DB**: Add bootstrap migration config and options ([#2519](https://github.com/m3db/m3/pull/2519))

# 0.15.8

## Misc

- **M3DB**: Pause rollout of background cold flush process by revert until further testing ([6830a8cb4](https://github.com/m3db/m3/commit/6830a8cb4))

# 0.15.7

## Performance

- **M3DB**: Background cold flush process to no longer block data snapshotting or commit log rotation ([#2460](https://github.com/m3db/m3/pull/2460))
- **M3DB**: Validate individual index entries on decode instead of entire file on open, to further improve bootstrap speed ([#2468](https://github.com/m3db/m3/pull/2468))

## Bug Fixes

- **M3DB**: Strict JSON unmarshal (disallow unknown fields) for raw HTTP/JSON DB node APIs ([#2490](https://github.com/m3db/m3/pull/2490))
- **M3Query**: Fix to regex selectors with leading wildcard ([#2505](https://github.com/m3db/m3/pull/#2505))

## Documentation

- **All**: Links to M3 meetup recordings ([#2494](https://github.com/m3db/m3/pull/2494))

# 0.15.6

## Features

- **M3DB**: Add per-namespace indexing runtime options to define concurrency weighted to indexing ([#2446](https://github.com/m3db/m3/pull/2446))

## Performance

- **M3DB**: Faster bootstrapping with deferred index checksum and significantly lower memory bump at block rotation ([#2446](https://github.com/m3db/m3/pull/2446))
- **M3DB**: Faster series aggregate metadata queries by intersecting postings term results with resolved query postings list ([#2441](https://github.com/m3db/m3/pull/2441))

## Bug Fixes

- **M3Query**: Fix for label matching behavior not the same as regular Prometheus for when label value is ".+" or ".*" ([#2479](https://github.com/m3db/m3/pull/2479))
- **M3Query**: Special case when request from Go process such as Prometheus Go client library for searching series metadata using Go min/max UTC values ([#2487](https://github.com/m3db/m3/pull/2487))
- **M3Query**: Auto-detect querying for entire retention time range ([#2483](https://github.com/m3db/m3/pull/2483))
- **M3Query**: Fix for Graphite query for metric with single identifier and no dot separated elements ([#2450](https://github.com/m3db/m3/pull/2450))

## Documentation

- **M3Coordinator**: Add rollup rules example documentation ([#2461](https://github.com/m3db/m3/pull/2461), [#2462](https://github.com/m3db/m3/pull/2462))

## Misc

- **M3DB**: Expose cluster total shards and replicas as metrics ([#2452](https://github.com/m3db/m3/pull/2452))

# 0.15.5

## Documentation

- **All**: Minor documentation fixes ([#2438](https://github.com/m3db/m3/pull/2438))
- **M3Query**: Add M3-Restrict-By-Tags-JSON example ([#2437](https://github.com/m3db/m3/pull/2437))

## Misc

- **M3DB**: Add continuous performance profiler that conditionally triggers with RPC endpoint ([#2416](https://github.com/m3db/m3/pull/2416))

# 0.15.4

## Features

- **M3DB**: Performance increases for block rotation by streamlining indexing lock contention ([#2423](https://github.com/m3db/m3/pull/2423))
- **M3DB**: Zero-copy of ID and fields on series index metadata re-indexing ([#2423](https://github.com/m3db/m3/pull/2423))
- **M3Coordinator**: Add ability to restrict and block incoming series based on tag matchers ([#2430](https://github.com/m3db/m3/pull/2430))

## Bug Fixes

- **M3DB**: Fix an error where background compaction caused transient errors in queries ([#2432](https://github.com/m3db/m3/pull/2432))

## Documentation

- **M3Query**: Update config settings and cleaned up documentation for per query limits ([#2427](https://github.com/m3db/m3/pull/2427))

# 0.15.3

## Features

- **M3DB**: Ability to set per-query block limit ([#2415](https://github.com/m3db/m3/pull/2415))
- **M3DB**: Ability to set global per-second query limit ([#2405](https://github.com/m3db/m3/pull/2405))

## Bug Fixes

- **M3DB**: Fix duplicate ID insertions causing transient error when flushing index block ([#2411](https://github.com/m3db/m3/pull/2411))
- **M3Coordinator**: Mapping rules with drop policies now correctly apply to unaggregated metrics ([#2262](https://github.com/m3db/m3/pull/2262))
- **M3Query**: Fix incorrect starting boundaries on some temporal queries ([#2413](https://github.com/m3db/m3/pull/2413))
- **M3Query**: Fix bug in one to one matching in binary functions ([#2417](https://github.com/m3db/m3/pull/2417))
- **M3DB**: Fix to edge case index data consistency on flush ([#2399](https://github.com/m3db/m3/pull/2399))

# 0.15.2

## Bug Fixes

- **M3DB**: Fix require exhaustive propagation of require exhaustive option through RPC ([#2409](https://github.com/m3db/m3/pull/2409))

# 0.15.1

## Features

- **M3DB**: Add ability to return an error when max time series limit is hit instead of partial result and warning ([#2400](https://github.com/m3db/m3/pull/2400))
- **M3Coordinator**: Add support for namespace retention updates by API ([#2383](https://github.com/m3db/m3/pull/2383))

## Bug Fixes

- **M3Coordinator**: Fix Content-Type for OpenAPI handler ([#2403](https://github.com/m3db/m3/pull/2403))
- **Build**: Build release binaries with goreleaser using Go 1.13 to match Go 1.13 docker images ([#2397](https://github.com/m3db/m3/pull/2397))

## Misc

- **M3DB**: Report a histogram of series blocks fetched per query ([#2381](https://github.com/m3db/m3/pull/2381))

# 0.15.0

## Features

- **M3Ctl**: Add M3 command line tool for calling APIs and using YAML files to apply commands ([#2097](https://github.com/m3db/m3/pull/2097))
- **M3Coordinator**: Add public API to write annotations (i.e. arbitrary bytes), next to datapoints for things like storing exemplars ([#2022](https://github.com/m3db/m3/pull/2022), [#2029](https://github.com/m3db/m3/pull/2029), [#2031](https://github.com/m3db/m3/pull/2031))
- **M3Coordinator**: Add support for mapping rules, allowing metrics to be stored at different resolutions based on their labels/tags ([#2036](https://github.com/m3db/m3/pull/2036))
- **M3Coordinator**: Add Graphite mapping rule support ([#2060](https://github.com/m3db/m3/pull/2060)) ([#2063](https://github.com/m3db/m3/pull/2063))
- **M3Coordinator**: Add community contributed InfluxDB write endpoint (at /api/v1/influxdb/write) ([#2083](https://github.com/m3db/m3/pull/2083))
- **M3Coordinator**: Add headers to pass along with request to remote write forward targets ([#2249](https://github.com/m3db/m3/pull/2249))
- **M3Coordinator**: Add retry to remote write forward targets ([#2299](https://github.com/m3db/m3/pull/2299))
- **M3Coordinator**: Add in-place M3Msg topic consumer updates with a PUT request ([#2186](https://github.com/m3db/m3/pull/2186))
- **M3Coordinator**: Add ability to rewrite tags for Prometheus remote write requests using header ([#2255](https://github.com/m3db/m3/pull/2255))
- **M3Coordinator**: Add config for multi-process launcher and SO_REUSEPORT listen servers for non-container based multi-process scaling ([#2292](https://github.com/m3db/m3/pull/2292))
- **M3Query**: Add Prometheus engine to compliment Prometheus Remote Read, improves performance by skipping serialization/deserialization/network overhead between Prometheus and M3Query ([#2343](https://github.com/m3db/m3/pull/2343), [#2369](https://github.com/m3db/m3/pull/2369))
- **M3Query**: Add header to support enforcing all queries in request to implicitly always include a given label/tag matcher ([#2053](https://github.com/m3db/m3/pull/2053))
- **M3Query**: Return headers indicating incomplete results for cross-regional fanout queries when remote fails or hits a limit ([#2053](https://github.com/m3db/m3/pull/2053))
- **M3Query**: Refactor query server to allow for custom handlers ([#2073](https://github.com/m3db/m3/pull/2073))
- **M3Query**: Add remote read debug parameters to look at raw data for a PromQL query and/or get results as JSON ([#2276](https://github.com/m3db/m3/pull/2276))
- **M3Query**: Add warnings for Prometheus queries to Prometheus query JSON response ([#2265](https://github.com/m3db/m3/pull/2265))
- **M3Query**: Add ability to set default query timeout by config ([#2226](https://github.com/m3db/m3/pull/2226))
- **M3Aggregator**: Add M3Msg aggregator client for high throughput point to point clustered buffered delivery of metrics to aggregator ([#2171](https://github.com/m3db/m3/pull/2171))
- **M3Aggregator**: Add rollup rule support for metrics aggregated with pre-existing timestamps, such as Prometheus metrics ([#2251](https://github.com/m3db/m3/pull/2251))
- **M3Aggregator**: Add aggregator passthrough functionality for aggregation in a local region forwarding to a remote region for storage ([#2235](https://github.com/m3db/m3/pull/2235))

## Performance

- **M3DB**: Improve RSS memory management with madvise resulting in flat RSS usage with a steady workload as time passes block-over-block ([#2037](https://github.com/m3db/m3/pull/2037))
- **M3DB**: Improve bootstrapping performance by allowing bootstrapping to be performed in a single pass, now possible for a lot of bootstraps to take just minutes depending on retention ([#1989](https://github.com/m3db/m3/pull/1989))
- **M3DB**: Use zero-copy references to index data instead of copy-on-read index data for each query, substantially improving query throughput and performance ([#1839](https://github.com/m3db/m3/pull/1839))
- **M3DB**: Further improve peer bootstrapping performance by using a document builder rather than raw memory segments ([#2078](https://github.com/m3db/m3/pull/2078))
- **M3DB**: Concurrent indexing when building segments for newly inserted metrics ([#2146](https://github.com/m3db/m3/pull/2146))
- **M3DB**: Decode ReadBits decompression improvements ([#2197](https://github.com/m3db/m3/pull/2197))
- **M3DB**: Remove implicit cloning of time ranges to reduce allocs ([#2178](https://github.com/m3db/m3/pull/2178))
- **M3Query**: Substantially improve temporal function performance ([#2049](https://github.com/m3db/m3/pull/2049))
- **M3Query**: Improve datapoint decompression speed ([#2176](https://github.com/m3db/m3/pull/2176), [#2185](https://github.com/m3db/m3/pull/2185), [#2190](https://github.com/m3db/m3/pull/2190))
- **M3Query**: Read bits uses an optimized byte reader ([#2205](https://github.com/m3db/m3/pull/2205))
- **M3Coordinator**: Ensure coordinator not grow M3Msg buffer if message over max size ([#2207](https://github.com/m3db/m3/pull/2207))

## Bug Fixes

- **M3Aggregator**: Take last value by wall clock timestamp not arrival time to avoid late arrivals overwriting actual later occuring values ([#2199](https://github.com/m3db/m3/pull/2199))
- **M3DB**: Validate indexable metrics for valid utf-8 prior to insert, also includes a utility for earlier M3DB versions to remove non-utf8 index data ([#2046](https://github.com/m3db/m3/pull/2046))
- **M3DB**: Remove incorrect error log message for missing schema with default non-protobuf namespaces ([#2013](https://github.com/m3db/m3/pull/2013))
- **M3DB**: Fixed memory leak causing index blocks to remain in memory after flushing ([#2037](https://github.com/m3db/m3/pull/2037))
- **M3DB**: Fix long standing possibility of double RLock acqusition ([#2128](https://github.com/m3db/m3/pull/2128))
- **M3DB**: Remove loop in fileset writer when previous fileset encountered an error writing out index files ([#2058](https://github.com/m3db/m3/pull/2058))
- **M3DB**: Instead of encountering an error skip entries for unowned shards in commit log bootstrapper ([#2145](https://github.com/m3db/m3/pull/2145))
- **M3DB**: Fix to avoid returning error when missing writable bucket with a cold flush ([#2188](https://github.com/m3db/m3/pull/2188))
- **M3DB**: Set defaults and expose configuration of TChannel timeouts, this avoids idle connection growth ([#2173](https://github.com/m3db/m3/pull/2173))
- **M3DB**: Account for Neg/Pos Offsets when building per field roaring bitmap posting lists ([#2213](https://github.com/m3db/m3/pull/2213))
- **M3DB**: Fix to build flush errors ([#2229](https://github.com/m3db/m3/pull/2229), [#2217](https://github.com/m3db/m3/pull/2217))
- **M3Coordinator**: Respect env and zone headers for topic API endpoints ([#2159](https://github.com/m3db/m3/pull/2159))
- **M3Coordinator**: Add support for Graphite Grafana plugin /find POST requests ([#2153](https://github.com/m3db/m3/pull/2153))
- **M3Coordinator**: Use tag options specified in config with M3Msg ingester ([#2212](https://github.com/m3db/m3/pull/2212))
- **M3Coordinator**: Only honor default aggregation policies if not matched by mapping rule ([#2203](https://github.com/m3db/m3/pull/2203))
- **M3Query**: Fix namespace resolve debug log not being written with multiple namespaces ([#2211](https://github.com/m3db/m3/pull/2211))
- **M3Query**: Fix to temporal function regression leading to inconsistent results ([#2231](https://github.com/m3db/m3/pull/2231))
- **M3Query**: Fix edge cases with cross-zonal query fanout and add verify utility ([#1993](https://github.com/m3db/m3/pull/1993))
- **M3Query**: Fix issue with histogram grouping ([#2247](https://github.com/m3db/m3/pull/2247))

## Documentation

- **M3Aggregator**: Add M3 aggregator Grafana dashboard ([#2064](https://github.com/m3db/m3/pull/2064))
- **M3Coordinator**: Add documentation to write to multiple clusters from a single coordinator ([#2187](https://github.com/m3db/m3/pull/2187))
- **M3DB**: Add documentation about estimating number of unique time series ([#2062](https://github.com/m3db/m3/pull/2062))
- **M3DB**: Update namespace configuration documentation to use simpler duration specific keys ([#2045](https://github.com/m3db/m3/pull/2045))

## Misc

- **All**: Upgrade to Go 1.13 and switch dependency management to Go modules ([#2221](https://github.com/m3db/m3/pull/2221))
- **All**: Add gauge metrics to measure the number of active routines for any worker pool ([#2061](https://github.com/m3db/m3/pull/2061))
- **All**: Allow for ${ENV_VAR_NAME} expansion with YAML configuration files ([#2033](https://github.com/m3db/m3/pull/2033))
- **All**: Add a utility for comparing performance and correctness across different versions of M3DB, enabling diffing the perf of different versions ([#2044](https://github.com/m3db/m3/pull/2044))
- **All**: Upgrade etcd client library to 3.4.3 ([#2101](https://github.com/m3db/m3/pull/2101))
- **All**: Include key name in watch errors ([#2138](https://github.com/m3db/m3/pull/2138))
- **Development**: Add HA Prometheus lab setup for dev M3 docker compose deployment ([#2206](https://github.com/m3db/m3/pull/2206))
- **Development**: Temporarily disable kubeval validation to allow builds on go 1.12 ([#2241](https://github.com/m3db/m3/pull/2241))
- **Development**: Add comparator value ingester for replaying functions against given data sets ([#2224](https://github.com/m3db/m3/pull/2224))
- **Development**: Logging improvements ([#2222](https://github.com/m3db/m3/pull/2222),[#2225](https://github.com/m3db/m3/pull/2225))
- **M3Aggregator**: Add a datasource variable and reuse it in all the panels of the aggregator dashboard ([#2182](https://github.com/m3db/m3/pull/2182))
- **M3DB**: Add client bad request/internal error distinction for metrics and sampled logs ([#2201](https://github.com/m3db/m3/pull/2201))
- **M3DB**: Add latency metrics to remote reads ([#2027](https://github.com/m3db/m3/pull/2027))
- **M3DB**: Add metrics for async replication worker pool utilization ([#2059](https://github.com/m3db/m3/pull/2059))
- **M3DB**: Remove carbon debug flag and rely on log debug level for debugging Carbon/Graphite mapping rules ([#2024](https://github.com/m3db/m3/pull/2024))
- **M3DB**: Add metric for BootstrappedAndDurable ([#2210](https://github.com/m3db/m3/pull/2210))
- **M3DB**: Use madvdontneed=1 in DB nodes to get a more accurate view of memory usage ([#2242](https://github.com/m3db/m3/pull/2242))
- **M3DB**: Add trace spans for database bootstrap process helping to identify all remaining slow code paths ([#2216](https://github.com/m3db/m3/pull/2216))
- **M3Coordinator**: Add power user API to custom set placement goal state for cluster membership and shards ([#2108](https://github.com/m3db/m3/pull/2108))
- **M3Coordinator**: Delete M3 aggregator related etcd keys when aggregator placement deleted ([#2133](https://github.com/m3db/m3/pull/2133))
- **M3Coordinator**: Add metrics for remote aggregator client and downsampler ([#2165](https://github.com/m3db/m3/pull/2165))
- **M3Coordinator**: Add aggregator client maxBatchSize config for configuring buffer for data sent to aggregator ([#2166](https://github.com/m3db/m3/pull/2166))
- **M3Query**: Allow both GET and POST for query APIs ([#2055](https://github.com/m3db/m3/pull/2055))
- **M3Query**: Only build amd64 architecture for m3query releases ([#2202](https://github.com/m3db/m3/pull/2202))

# 0.14.2

## Bug Fixes

- **M3DB**: Fix the persist cycle not cleaning up state for reuse when flush times cannot be calculated ([#2007](https://github.com/m3db/m3/pull/2007))
- **M3Query**: Add specialized matchers for empty EQ/NEQ matchers ([#1986](https://github.com/m3db/m3/pull/1986))

## Misc

- **M3Aggregator**: Do not require aggregator ID to be joined with port and add instance initialization debug logs ([#2012](https://github.com/m3db/m3/pull/2012))
- **All**: Support env var expansion using [go.uber.org/config](go.uber.org/config) ([#2016](https://github.com/m3db/m3/pull/2016))
- **All**: Deprecate listen address expansion in favor of [go.uber.org/config](go.uber.org/config) env var expansion ([#2017](https://github.com/m3db/m3/pull/2017))

# 0.14.1

## Features

- **M3Query**: Add endpoint that parses query to an AST ([#2002](https://github.com/m3db/m3/pull/2002))

## Bug Fixes

- **M3Query**: Fix label replace lazy block function execution ([#1985](https://github.com/m3db/m3/pull/1985))

## Misc

- **Build**: Ensure CGO is disabled in release binaries ([#2005](https://github.com/m3db/m3/pull/2005))

# 0.14.0

## Features

- **M3Query**: Add a limit exceeded header if too many series are matched for a query when limit is specified ([#1838](https://github.com/m3db/m3/pull/1838))
- **M3Query**: Graphite datapoint timestamps are now truncated to resolution for deterministic steps independent of start/end query time ([#1997](https://github.com/m3db/m3/pull/1997))
- **M3Coordinator**: Endpoint for /debug/dump now returns placement/namespace of the current configured database ([#1981](https://github.com/m3db/m3/pull/1981))
- **M3DB**: Add support for batching writes across namespaces ([#1974](https://github.com/m3db/m3/pull/1974))
- **M3DB**: Add support for batching fetches across namespaces ([#1987](https://github.com/m3db/m3/pull/1987))

## Performance

- **M3DB**: Optimize fetches to avoid decoding more data than required to satisfy consistency ([#1966](https://github.com/m3db/m3/pull/1966))

## Bug Fixes

- **M3DB**: Fix merge checksumming logic ([#1988](https://github.com/m3db/m3/pull/1988))

## Misc

- **M3Aggregator**: Add aggregator to release binaries ([#1943](https://github.com/m3db/m3/pull/1943))

# 0.13.0

## Features

- **M3Query**: Federated queries (e.g. for cross region/zone queries) configurable to return a warning and partial results than a hard error to route around unhealthy regions ([#1938](https://github.com/m3db/m3/pull/1938))
- **M3Query**: Add ability to turn on GRPC reflection for testing query endpoints using utilities like grpcurl ([#1856](https://github.com/m3db/m3/pull/1856))
- **M3Query**: Add ability to use multiple config files for config overrides ([#1934](https://github.com/m3db/m3/pull/1934))
- **M3DB**: Add ability to replicate writes best effort directly to multiple clusters by specifying multiple etcd clusters for M3DB client ([#1859](https://github.com/m3db/m3/pull/1859))
- **M3Coordinator**: Add ability to replicate Prometheus remote write requests by forwarding the compressed body to downstream remote write endpoints ([#1922](https://github.com/m3db/m3/pull/1922), [#1940](https://github.com/m3db/m3/pull/1940))

## Bug Fixes

- **M3Coordinator**: Fix ability to delete coordinator placements ([#1918](https://github.com/m3db/m3/pull/1918))
- **M3Aggregator**: Ensure flush manager worker pool is at least size 1 irregardless of num CPUs ([#1881](https://github.com/m3db/m3/pull/1881))

## Documentation

- **M3**: Add FAQ and seed with some frequently asked questions asking ([#1927](https://github.com/m3db/m3/pull/1927))
- **M3Coordinator**: Fix the cluster placement endpoint documentation in the manual cluster setup instructions ([#1936](https://github.com/m3db/m3/pull/1936))

# 0.12.0

## Features

- **M3DB**: Add support for limiting the amount of outstanding repaired data that can be held in memory at once ([#1885](https://github.com/m3db/m3/pull/1885))
- **M3Coordinator**: Add namespace and placement information to the dump returned by the debug endpoint ([#1896](https://github.com/m3db/m3/pull/1896))
- **M3Coordinator**: Add DELETE api for the /topic endpoint ([#1926](https://github.com/m3db/m3/pull/1926))
- **M3Aggregator:** Make YAML serialization roundtrip for config related types ([#1864](https://github.com/m3db/m3/pull/1864))

## Performance

- **M3Query**: Improve performance of temporal functions ([#1917](https://github.com/m3db/m3/pull/1917))
- **M3DB**: Only trigger bootstrap during topology change if a node receives new shards. This should significantly reduce the amount of time certain topology changes (such as node removes) took when performed on larger clusters ([#1868](https://github.com/m3db/m3/pull/1868))

# 0.11.0

## Migration Disclaimer

Version 0.11.0 of M3 includes further work on supporting writing data at arbitrary times (within the retention period). While most of these changes are transparent to the user in terms of functionality and performance, we had to make a change to the naming format of the files that get persisted to disk ([#1720](https://github.com/m3db/m3/pull/1720)). This change was required to handle multiple fileset volumes per block, which is necessary after introducing "cold flushes" ([#1624](https://github.com/m3db/m3/pull/1624)).

The redesign is **backwards compatible** but not **forwards compatible**. This means that you should be able upgrade your < 0.11.0 clusters to 0.11.0 with no issues, but you will not be able to downgrade without taking some additional steps.

### Troubleshooting and Rolling Back

If you run into any issues with the upgrade or need to downgrade to a previous version for any reason, follow these steps:

1. Stop the node that is having trouble with the upgrade or that you're trying to downgrade.
2. Remove the filesets that include a volume index in them, e.g. this is a filename with the new volumed format: `fileset-1257890400000000000-0-data.db`, and this is the corresponding filename in the original format: `fileset-1257890400000000000-data.db`.
3. Modify the `bootstrappers` config in the M3DB YAML file from `filesystem, commitlog, peers, uninitialized_topology` to `filesystem, peers, commitlog, uninitialized_topology`. This will force the node to bootstrap from its peers instead of the local snapshot and commitlog files it has on disk, which is important otherwise when the node restarts, it will think that it has already been bootstrapped.
4. Turn the node back on.

## Features

- **M3DB**: Add cluster repair capability for non-indexed data ([#1831](https://github.com/m3db/m3/pull/1831), [#1849](https://github.com/m3db/m3/pull/1849), [#1862](https://github.com/m3db/m3/pull/1862), [#1874](https://github.com/m3db/m3/pull/1874))
- **M3DB**: Enable arbitrary out of order writes for non-indexed data ([#1780](https://github.com/m3db/m3/pull/1780), [#1790](https://github.com/m3db/m3/pull/1790), [#1829](https://github.com/m3db/m3/pull/1829))
- **M3DB**: Expose debug dump on /debug/dump for zip file of profiles and data about the environment for debugging ([#1811](https://github.com/m3db/m3/pull/1811))
- **M3Query**: Allow lookback duration to be set in query parameters for configurable lookback windowing ([#1793](https://github.com/m3db/m3/pull/1793))

## Performance

- **M3DB**: Improve base line write performance using pre-encoded tags instead of encoding just before commit log write ([#1898](https://github.com/m3db/m3/pull/1898), [#1904](https://github.com/m3db/m3/pull/1904))
- **M3DB**: Avoid allocating expensive digest readers in seek.Open() ([#1835](https://github.com/m3db/m3/pull/1835))

## Bug Fixes

- **M3Query**: Fix PromQL variadic functions with no params specified ([#1846](https://github.com/m3db/m3/pull/1846))
- **M3Query**: Fix PromQL absent function ([#1871](https://github.com/m3db/m3/pull/1871))
- **M3Query**: Fix PromQL binary comparisons that use the time function ([#1888](https://github.com/m3db/m3/pull/1888))
- **M3Query**: Fix multi-deployment queries (e.g. cross region) using temporal functions ([#1901](https://github.com/m3db/m3/pull/1901))
- **M3DB**: Index queries to local node now returns only IDs owned by the node at time of read ([#1822](https://github.com/m3db/m3/pull/1822))
- **M3DB**: Automatically create folder for KV cache, avoiding common error logs ([#1757](https://github.com/m3db/m3/pull/1757))
- **M3Query**: Batch remote server responses to avoid large individual RPC messages ([#1784](https://github.com/m3db/m3/pull/1784))
- **M3DB**: Use rlimit and setcap together to raise file descriptor limits to desired setting if possible ([#1792](https://github.com/m3db/m3/pull/1792), [#1745](https://github.com/m3db/m3/pull/1745), [#1850](https://github.com/m3db/m3/pull/1850))

## Documentation

- **M3DB**: Add shard and recommended block sizes ([#1890](https://github.com/m3db/m3/pull/1890))
- **M3Coordinator**: Improve namespace specific read/write header documentation ([#1884](https://github.com/m3db/m3/pull/1884))

# 0.10.2

## Performance

- **M3DB**: First of a set of incremental bootstrap improvements, improve commit log reader speed ([#1724](https://github.com/m3db/m3/pull/1724))

## Bug Fixes

- **M3Query**: Add verbose step size parsing errors to identify problematic queries ([#1734](https://github.com/m3db/m3/pull/1734))
- **M3Coordinator**: Add verbose errors to identify metrics arriving too late for aggregation and make lateness policies able to be set by config ([#1731](https://github.com/m3db/m3/pull/1731))

# 0.10.1

## Bug Fixes

- **M3Coordinator**: Use table based approach for aggregation tile buffer past calculation with good defaults for Prometheus remote write latency ([#1717](https://github.com/m3db/m3/pull/1717))

# 0.10.0

## Features

- **M3Query**: Add multi-zone and multi-region configuration for coordinator ([#1687](https://github.com/m3db/m3/pull/1687))
- **M3Query**: Add debug param to `GET` `/api/v1/namespace` endpoint for better readability ([#1698](https://github.com/m3db/m3/pull/1698))
- **M3Coordinator**: Add "ingest_latency" histogram metric and return datapoint too old/new errors with offending timestamps ([#1716](https://github.com/m3db/m3/pull/1716))

## Performance

- **M3DB**: Add forward index write capability which eases index pressure at block boundaries ([#1613](https://github.com/m3db/m3/pull/1613))
- **M3Query**: Drop empty series from appearing in output when `keepNaNs` option is set to disabled ([#1682](https://github.com/m3db/m3/pull/1682), [#1684](https://github.com/m3db/m3/pull/1684))
- **M3DB**: Build and release M3DB with Go 1.12 ([#1674](https://github.com/m3db/m3/pull/1674))

## Bug Fixes

- **M3DB**: Fix a bug where peer bootstrapping would sometimes get stuck for a short period of time due to other M3DB nodes (temporarily) returning more than one block of data for a given time period ([#1707](https://github.com/m3db/m3/pull/1707))
- **M3DB**: Fix a bug that would cause multiple orders of magnitude slow-down in peer bootstrapping / node replacements when one of the nodes in the cluster was hard down ([#1677](https://github.com/m3db/m3/pull/1677))
- **M3Query**: Fix a bug with parsing Graphite find queries by adding `MatchField` and `MatchNotField` match types, and explicitly making use of them in graphite queries ([#1676](https://github.com/m3db/m3/pull/1676))
- **M3Query**: Propagate limit settings when using Prometheus Remote Read ([#1685](https://github.com/m3db/m3/pull/1685))
- **M3Coordinator**: Return a 404 rather than a 500 if we attempt to delete a nonexistent placement ([#1701](https://github.com/m3db/m3/pull/1701))
- **M3Coordinator**: Return HTTP 400 if all sent samples encounter too old/new or other bad request error ([#1692](https://github.com/m3db/m3/pull/1692))

# 0.9.6

## Bug Fixes

- **M3DB**: Revert Docker setcap change to allow image to run with no special flags ([#1672](https://github.com/m3db/m3/pull/1672))

# 0.9.5

## Performance

- **M3DB**: Use ReadAt() pread syscall to reduce syscalls per time series data read and half the number of open FDs with default configuration ([#1664](https://github.com/m3db/m3/pull/1664))

## Bug Fixes

- **M3Query**: Add missing support for PromQL unary expressions ([#1647](https://github.com/m3db/m3/pull/1647))
- **M3DB**: Call setrlimit to set open FDs hard limit for docker image and in Dockerfile use setcap to enable capability ([#1666](https://github.com/m3db/m3/pull/1666))
- **M3DB**: Updated Tally to 3.3.10 to issue correct error message when listen address conflict occurs with Prometheus reporter ([#1660](https://github.com/m3db/m3/pull/1660))

# 0.9.4

## Bug Fixes

- **M3Query**: Adding complete tags query fanout support with integration test ([#1656](https://github.com/m3db/m3/pull/1656))

# 0.9.3

## Bug Fixes

- **M3Query**: Add docker integration test for query fanout and fix query aggregating results ([#1652](https://github.com/m3db/m3/pull/1652))
- **M3DB**: Restore process stats disabled by the open FD counting performance fix ([#1648](https://github.com/m3db/m3/pull/1648))

# 0.9.2

## Performance

- **M3Query**: Add default and config to limit the number of unique time series fetch from a single DB node to fulfill a query (default 10,000) ([#1644](https://github.com/m3db/m3/pull/1644))
- **M3Query**: Fix and throttle file descriptor stat emission excessive allocation, also disable Prometheus process collector which had the same issue ([#1633](https://github.com/m3db/m3/pull/1633))

## Bug Fixes

- **M3DB**: Fix ReturnUnfulfilledForCorruptCommitLogFiles not propagating to bootstrapper options, causing peer bootstrapper to begin when it should not ([#1639](https://github.com/m3db/m3/pull/1639))
- **M3Query**: Fix PromReadInstantHandler not correctly responding with error message when query encounters error ([#1611](https://github.com/m3db/m3/pull/1611))

# 0.9.1

## Performance

- **M3DB**: Use index aggregate query to speed up the [/label/<label_name>/values endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values) ([#1628](https://github.com/m3db/m3/pull/1628))

## Bug Fixes

- **M3Query**: Fixed an issue causing untyped step sizes to fail parsing, now defaults to using seconds as the time unit ([#1617](https://github.com/m3db/m3/pull/1617))
- **M3Query**: Accept both GET and POST for series match endpoint ([#1606](https://github.com/m3db/m3/pull/1606))

# 0.9.0

## Features

- **M3Coordinator**: Emit caller I.P address in error logs ([#1583](https://github.com/m3db/m3/pull/1583))
- **M3Query**: Add a list tags endpoint that displays every unique tag name stored in M3DB to match the Prometheus [/labels endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names) ([#1565](https://github.com/m3db/m3/pull/1565))
- **M3DB**: Add native support for running M3DB in "index-only" mode ([#1596](https://github.com/m3db/m3/pull/1596))
- **M3DB**: Reduce M3DB log spam for normal operations like cleanup ([#1592](https://github.com/m3db/m3/pull/1592))

## Bug Fixes

- **M3DB**: Filter out commit log files that were not present on disk before the process started when determining if files are corrupt in the commitlog bootstrapper. This will prevent a race in which the commitlog bootstrapper would interpret the current commitlog file as corrupt ([#1581](https://github.com/m3db/m3/pull/1581))
- **M3Query**: Fixed binary arithmetic to properly ignore names when checking for matching series ([#1604](https://github.com/m3db/m3/pull/1604))
- **M3Query**: Add start and end time to tag values endpoint to fix an issue preventing Grafana tag completion ([#1601](https://github.com/m3db/m3/pull/1601))

## Performance

- **M3DB**: Optimize some types aggregation queries in M3DB to not require materializing intermediary documents. This should substantially improve query performance for certain types of aggregation queries such as those used to perform auto-complete in Prometheus and Grafana ([#1545](https://github.com/m3db/m3/pull/1545))
- **M3DB**: Open new commitlog files asynchronously. This should prevent large spikes in the commitlog queue during snapshotting / commitlog rotation for write-heavy workloads ([#1576](https://github.com/m3db/m3/pull/1576))

# 0.8.4 (2019-04-20)

## Performance

- **M3DB**: Fix index query and aggregate query pooling using finalizers ([#1567](https://github.com/m3db/m3/pull/1567))

## Bug Fixes

- **M3Query**: Fix PromQL `offset` modifier implementation ([#1561](https://github.com/m3db/m3/pull/1561))
- **M3DB**: Fix index flush recovery when previous index flush attempts have failed ([#1574](https://github.com/m3db/m3/pull/1574))

# 0.8.3 (2019-04-12)

## Performance

- **M3DB**: Bump TagLiteralLength limit to 64K ([#1552](https://github.com/m3db/m3/pull/1552))

# 0.8.2 (2019-04-11)

## New Features

- **M3DB**: Add Jaeger tracing to M3DB QueryIDs path ([#1506](https://github.com/m3db/m3/pull/1506))

## Performance

- **M3DB**: Add postingsList caching for FieldQuery ([#1530](https://github.com/m3db/m3/pull/1530))
- **M3DB**: Add write & read latency histogram metrics to M3DB client and emit from M3Coordinator ([#1533](https://github.com/m3db/m3/pull/1533))
- **M3DB**: Persist block at retention edge when building index after peer streaming ([#1531](https://github.com/m3db/m3/pull/1531))

## Bug Fixes

- **M3Query**: Fixed a bug that could cause a panic when converting queries ([#1546](https://github.com/m3db/m3/pull/1546))

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

Operators may need to tune their kernel configuration to allow a higher number of open file descriptors. Please follow our [Kernel Configuration Guide](https://docs.m3db.io/operational_guide/kernel_configuration/) for more details.

## New Features

- **M3DB**: Add dedicated aggregation endpoint ([#1463](https://github.com/m3db/m3/pull/1463), [#1502](https://github.com/m3db/m3/pull/1502), [#1483](https://github.com/m3db/m3/pull/1483))

## Performance

- **M3DB**: Use Seek() and Read() APIs for index/data files instead of mmap + share expensive seeker resources among seekers ([#1421](https://github.com/m3db/m3/pull/1421))
- **M3Query**: Tag completion endpoints (Prometheus (`match[]`, `/{foo}/series`), and Graphite (`find`) endpoints) now use the new dedicated aggregation endpoint ([#1481](https://github.com/m3db/m3/pull/1481))

# 0.7.3 (2019-03-22)

## New Features

- **M3DB**: Add an AllQuery index query type ([#1478](https://github.com/m3db/m3/pull/1478))

## Bug Fixes

- **M3DB**: Fix to annotation pooling ([#1476](https://github.com/m3db/m3/pull/1476))
- **M3Coordinator**: Only panics log a stacktrace rather than expected errors ([#1480](https://github.com/m3db/m3/pull/1480))

## Performance

- **M3DB**: Use a single results object for merging postings lists across blocks, rather than creating a result per block and discarding them immediately thereafter. This should reduce memory utilization significantly for queries that look back over a large number of blocks ([#1474](https://github.com/m3db/m3/pull/1474))
- **M3DB**: Improvement to applying back pressure on writes with a full commitlog, which should improve M3DB's ability to apply back pressure under load and recover quickly after being overwhelmed with traffic. ([#1482](https://github.com/m3db/m3/pull/1482))

# 0.7.2 (2019-03-15)

## Bug Fixes

- **All Binaries**: Fix LD flags in release so that version, branch, build date, etc are packaged into the binary properly.

# 0.7.1 (2019-03-15)

## New Features

- **M3Query**: Add per-query cost accounting to allow fine-grained controls on datapoint limits to Prometheus and Graphite style queries ([#1207](https://github.com/m3db/m3/pull/1207)) ([#1449](https://github.com/m3db/m3/pull/1449))
- **M3Query**: Add optional pickled return type for Graphite render endpoint ([#1446](https://github.com/m3db/m3/pull/1446))
- **M3Query**: Drop NaNs from query results ([#1458](https://github.com/m3db/m3/pull/1458))

## Bug Fixes

- **M3DB**: Fix bug in postings list caching which could cause incorrect results on queries ([#1461](https://github.com/m3db/m3/pull/1461))

# 0.7.0 (2019-03-12)

## Migration Disclaimer

Version 0.7.0 of M3 includes a redesign of the snapshotting and commitlog components ([#1384](https://github.com/m3db/m3/pull/1384) among others). This redesign was critical to improve M3DB's consistency guarantees, reduce the amount of disk space that is wasted by commitlogs, and enable future feature development to support writing data at arbitrary times.

The redesign is **backwards compatible** but not **forwards compatible**. This means that you should be able upgrade your < 0.7.0 clusters to 0.7.0 with no issues, but you will not be able to downgrade without taking some additional steps. Note that the first bootstrap after the upgrade may take longer than usual, but subsequent bootstraps should be just as fast as they used to be, or even faster.

### Troubleshooting and Rolling Back

If you run into any issues with the upgrade or need to downgrade to a previous version for any reason, follow these steps:

1. Stop the node that is having trouble with the upgrade or that you're trying to downgrade.
2. Modify the `bootstrappers` config in the M3DB YAML file from `filesystem, commitlog, peers, uninitialized_topology` to `filesystem, peers, commitlog, uninitialized_topology`. This will force the node to bootstrap from its peers instead of the local snapshot and commitlog files it has on disk, bypassing any issues related to file incompatibility between versions.
3. Turn the node back on and wait for it to finish bootstrapping and snapshotting. Once everything looks stable, change the config back to `filesystem, commitlog, peers, uninitialized_topology` so that the next time the node is restarted it will default to using the snapshot and commitlog files.

## New Features

- **M3DB**: Restructuring of commitlog and snapshotting feature as described above ([#1384](https://github.com/m3db/m3/pull/1384))
- **M3DB**: Obtain a lock on data directory on startup ([#1376](https://github.com/m3db/m3/pull/1376))
- **M3Coordinator**: Add support for zone / environment override headers in namespace APIs so multiple M3DB clusters can be administered from the same M3Coordinator instance / etcd cluster ([#1427](https://github.com/m3db/m3/pull/1427))
- **M3Query**: Add Jaeger tracing to M3Query, and mechanism to plugin custom opentracing implementations ([#1321](https://github.com/m3db/m3/pull/1321))
- **M3nsch**: Add basic Grafana dashboard showing success rates and latencies ([#1401](https://github.com/m3db/m3/pull/1401))
- **M3nsch**: Support generating new unique metrics ([#1397](https://github.com/m3db/m3/pull/1397))

## Performance

- **M3DB**: Optimize OStream implementation which reduces CPU synchronization for each write. Should result in modest improvement in load average for metrics workloads and a massive improvement in load average for any workload using large annotations ([#1399](https://github.com/m3db/m3/pull/1399), [#1437](https://github.com/m3db/m3/pull/1437))
- **M3DB**: Prevent duplicate writes from being written to the commitlog ([#1375](https://github.com/m3db/m3/pull/1375))
- **M3DB**: Construct RPC service once and share it with TChannel and HTTP servers to prevent pools from being initialized multiple times reducing memory footprint ([#1420](https://github.com/m3db/m3/pull/1420))
- **M3Query**: Add LRU cache for query conversion. Should help dashboards with expensive regex query in particular ([#1398](https://github.com/m3db/m3/pull/1398))

## Bug Fixes

- **M3Coordinator**: Better error responses from namespace APIs when namespace is unknown ([#1412](https://github.com/m3db/m3/pull/1412))
- **M3Query**: Fix panic in temporal functions ([#1429](https://github.com/m3db/m3/pull/1429))

# 0.6.1 (2019-02-20)

## Bug fixes

- **M3Coordinator**: Fix to panic caused by generating ID for series with no tags ([#1392](https://github.com/m3db/m3/pull/1392))
- **M3Coordinator**: Fix to panic caused by reading placement when none available instead of return 404 ([#1391](https://github.com/m3db/m3/pull/1391))

# 0.6.0 (2019-02-19)

## Breaking changes

- **M3Coordinator**: ID generation scheme must be explicitly defined in configs ([Set "legacy" if unsure, further information on migrating to 0.6.0](https://docs.m3db.io/how_to/query/#migration)) ([#1381](https://github.com/m3db/m3/pull/1381))

## New Features

- **M3DB** (Config): Simplify M3 config options ([#1371](https://github.com/m3db/m3/pull/1371))
- **M3Coordinator**: Improve database creation API ([#1350](https://github.com/m3db/m3/pull/1350))
- **M3Query**: Add quantile_over_time and histogram_quantile Prometheus functions ([#1367](https://github.com/m3db/m3/pull/1367), [#1373](https://github.com/m3db/m3/pull/1373))
- **Documentation**: Additional documentation for namespace setup and configuration, etcd, and M3Coordinator ID generations schemes ([#1350](https://github.com/m3db/m3/pull/1350), [#1354](https://github.com/m3db/m3/pull/1354), [#1381](https://github.com/m3db/m3/pull/1381), [#1385](https://github.com/m3db/m3/pull/1385))

## Performance

- **M3DB** (Index): Add posting list cache that should result in a massive improvement for M3DB query performance for most workloads ([#1370](https://github.com/m3db/m3/pull/1370))

## Bug fixes

- **M3DB** (Index): Fix race condition in index query ([#1356](https://github.com/m3db/m3/pull/1356))
- **M3Coordinator**: Fix panic responder which was previously not reporting query panics to users ([#1353](https://github.com/m3db/m3/pull/1353))
- **M3Query**: Fix bug in calculating temporal (over_time) functions ([#1271](https://github.com/m3db/m3/pull/1271))

# 0.5.0 (2019-02-03)

## New Features

- **M3Coordinator**: Add [Graphite support](https://docs.m3db.io/integrations/grafana/) in the form of Carbon ingestion (with configurable aggregation and storage policies), as well as direct and Grafana based Graphite querying support ([#1309](https://github.com/m3db/m3/pull/1309), [#1310](https://github.com/m3db/m3/pull/1310), [#1308](https://github.com/m3db/m3/pull/1308), [#1319](https://github.com/m3db/m3/pull/1319), [#1318](https://github.com/m3db/m3/pull/1318), [#1327](https://github.com/m3db/m3/pull/1327), [#1328](https://github.com/m3db/m3/pull/1328))
- **M3Coordinator**: Add tag completion API ([#1175](https://github.com/m3db/m3/pull/1175))
- **M3Coordinator**: Add new opt-in ID generation function that will never collide ([#1286](https://github.com/m3db/m3/pull/1286))
- **M3DB**: Add [endpoint](https://m3db.io/openapi/#operation/databaseConfigSetBootstrappers) for setting database bootstrapers dynamically([#1239](https://github.com/m3db/m3/pull/1239))

## Performance

- **M3DB** (Index) Replace usage of slow "memory segment" for index segment with immutable F.S.Ts that are constantly being generated in the foreground as series are being inserted. Significantly reduces query latency (10x+) for some types of workloads that make heavy use of regex ([#1197](https://github.com/m3db/m3/pull/1197))
- **M3DB**: (Index) Add support for concurrent index block queries (improves performance of queries that look back many blocks ([#1195](https://github.com/m3db/m3/pull/1195))
- **M3DB**: (Index) Improve pooling configuration of one of the index array pools to prevent (almost) unbounded growth over time for certain workloads ([#1254](https://github.com/m3db/m3/pull/1254))
- **M3DB**: (Index) Use only one roaring bitmap library (Pilosa), and upgrade to version of Pilosa with our upstreammed `UnionInPlace` improvements that reduces memory consumption of expensive queries by over a factor of 10x ([#1238](https://github.com/m3db/m3/pull/1238)), fixes: [#1192](https://github.com/m3db/m3/pull/1192)
- **M3DB**: (Index) Don't use object pool for allocating long-lived arrays of tag slices which reduces steady-state memory consumption because the default size is 16 which is much bigger than the number of tags most metrics have ([#1300](https://github.com/m3db/m3/pull/1300))
- **M3DB**: Auto-calculate size of WriteBatchPool based on commitlog queue size and improve chance of batch being returned to pool ([#1236](https://github.com/m3db/m3/pull/1236))
- **M3DB**: Don't allow msgpack library to allocate \*bufio.Reader (reduces allocations) and mmap bloomfilters and index summaries as files instead of anonymously so they can be paged out by the O.S if necessary ([#1289](https://github.com/m3db/m3/pull/1289))

## Bug fixes

- **M3DB**: Fix bug where namespace-level configuration "writes to commitlog" was not respected ([#1232](https://github.com/m3db/m3/pull/1232))
- **M3DB**: Improve how M3DB handles durability during topology changes. Previously, a new node that was added to an M3DB cluster would not be completely durable (able to recover all data from disk) for a few minutes after the node join completed even though it marked all its shards as available. Now, newly added M3DB nodes will never mark their shards as available until they are completely durable ([#1183](https://github.com/m3db/m3/pull/1183)). Also, Make M3DB health check not return success until node is bootstrapped AND durable, not just bootstrapped. This makes automated operations (like those performed by the Kubernetes operator or various scripts) much safer ([#1287](https://github.com/m3db/m3/pull/1287))
- **M3DB**: Make it possible to safely replace/add/remove M3DB seed nodes by exposing etcd configuration ([#1339](https://github.com/m3db/m3/pull/1339))
- **M3Coordinator**: Fix bug in database create API so it respects number of shards ([#1188](https://github.com/m3db/m3/pull/1188))
- **M3Coordinator**: Fix tag propagation for temporal functions ([#1307](https://github.com/m3db/m3/pull/1307))
- **M3Coordinator**: Properly propagate M3DB fetch timeout from config instead of using default value ([#1342](https://github.com/m3db/m3/pull/1342))

# 0.4.8 (2018-10-20)

- **Coordinator**: Reduce log spam for high latency requests ([#1164](https://github.com/m3db/m3/pull/1164))
- **Coordinator**: Add support for replace API for db nodes ([#1162](https://github.com/m3db/m3/pull/1162))
- **Coordinator**: Add sort and sort_desc and linear regression promql functions ([#1104](https://github.com/m3db/m3/pull/1104), [#1063](https://github.com/m3db/m3/pull/1063))
- **DB**: Emit logs when process limits are misconfigured ([#1118](https://github.com/m3db/m3/pull/1118))
- **DB**: Change the implementation of the snapshotting process to snapshot all unflushed blocks ([#1017](https://github.com/m3db/m3/pull/1017))
- **DB**: Remove CacheAllMetadata policy ([#1110](https://github.com/m3db/m3/pull/1110))
- FIX **Coordinator**: Fixed bug in database creation API: error not displayed in logs ([#1089](https://github.com/m3db/m3/pull/1089))
- FIX **DB**: Various changes that improve M3DBs resiliency to corrupt commitlog files during bootstrap and cleanup as well as its resiliency to corrupt fileset files ([#1065](https://github.com/m3db/m3/pull/1065), [#1066](https://github.com/m3db/m3/pull/1066), [#1086](https://github.com/m3db/m3/pull/1086))
- FIX **Coordinator**: Fix OpenAPI yml, was previously broken and would not render ([#1062](https://github.com/m3db/m3/pull/1062))
- PERF **DB**: Sample M3DB write method timers to improve performance for high write throughput workloads ([#1057](https://github.com/m3db/m3/pull/1057))
- PERF **DB**: Massive improvement in write throughput (2-3 in some cases) by improving the speed of the commitlog msgpack encoder and removing contention on the commitlog queue via batching ([#1160](https://github.com/m3db/m3/pull/1160), [#1157](https://github.com/m3db/m3/pull/1157))

# 0.4.7 (2018-10-10)

- **Aggregator, Collector**: Add m3aggregator and m3collector for clustered downsampling (440b41657, df3999d58, [#1030](https://github.com/m3db/m3/pull/1030), [#1038](https://github.com/m3db/m3/pull/1038), [#1050](https://github.com/m3db/m3/pull/1050), [#1061](https://github.com/m3db/m3/pull/1061))
- **Coordinator**: Add m3msg server and placement and topic APIs in m3coordinator to enable use as backend with m3aggregator ([#1028](https://github.com/m3db/m3/pull/1028), [#1055](https://github.com/m3db/m3/pull/1055), [#1060](https://github.com/m3db/m3/pull/1060))
- DOCS **DB**: Add doc links to placement and namespace config operational guides ([#1029](https://github.com/m3db/m3/pull/1029))

# 0.4.6 (2018-10-05)

- **Coordinator**: Add cluster namespace fanout heuristics supporting queries greater than retention ([#908](https://github.com/m3db/m3/pull/908))
- **Coordinator**: Add ability for query storage to provide unconsolidated blocks ([#929](https://github.com/m3db/m3/pull/929))
- FIX **Coordinator**: Multi-fetch fixes ([#989](https://github.com/m3db/m3/pull/989))
- FIX **Coordinator**: Disable CGO on linux builds ([#969](https://github.com/m3db/m3/pull/969))
- FIX **Coordinator**: Write fanouts with aggregated namespaces ([#991](https://github.com/m3db/m3/pull/991))
- PERF **Coordinator**: Set namespace and ID as NoFinalize ([#956](https://github.com/m3db/m3/pull/956))
- PERF **Coordinator**: Treat Prometheus TagName/Value as []byte instead of String ([#1004](https://github.com/m3db/m3/pull/1004))
- PERF **Coordinator**: Improve performance of generatings IDs from tags in M3Coordinator ([#1000](https://github.com/m3db/m3/pull/1000))
- PERF **Coordinator**: Significantly improve performance of FetchResultToPromResult and helper functions ([#1003](https://github.com/m3db/m3/pull/1003))
- PERF **Coordinator**: Improve M3DB session performance part 2: Dont clone IDs if they are IsNoFinalize() ([#986](https://github.com/m3db/m3/pull/986))
- PERF **Coordinator**: Improve M3DB session performance part 1: Pool goroutines in host queues ([#985](https://github.com/m3db/m3/pull/985))
- PERF **Coordinator**: Use pooling for writes to coordinator ([#942](https://github.com/m3db/m3/pull/942))
- PERF **Coordinator**: Use a better pool size for coordinator writes ([#976](https://github.com/m3db/m3/pull/976))
- PERF **Coordinator**: Avoid duplicate ident.ID conversions ([#935](https://github.com/m3db/m3/pull/935))
- DOCS **DB**: Add placement change operational guide ([#998](https://github.com/m3db/m3/pull/998))
- DOCS **DB**: Add namespace modification operational guide ([#995](https://github.com/m3db/m3/pull/995))
- DOCS **DB**: [integrations] systemd: add config flag to service ([#974](https://github.com/m3db/m3/pull/974))
- DOCS **DB**: Add operational guides for M3DB (topology and bootstrapping) ([#924](https://github.com/m3db/m3/pull/924))
- DOCS **DB**: [integrations] systemd: add systemd unit example ([#970](https://github.com/m3db/m3/pull/970))
- DOCS **DB**: Update bootstrapping operational guide ([#967](https://github.com/m3db/m3/pull/967))
- DOCS **DB**: Timeout topology watch and return better error messages for missing topology / namespaces ([#926](https://github.com/m3db/m3/pull/926))
- DOCS **DB**: Add Prom/Grafana dashboard for M3DB as well as docker-compose for local development of m3-stack ([#939](https://github.com/m3db/m3/pull/939))

# 0.4.5 (2018-09-24)

- FIX **DB** Index data race in FST Segment reads ([#938](https://github.com/m3db/m3/pull/938))

# 0.4.4 (2018-09-21)

- **DB:** Use commit log bootstrapper before peers bootstrapper ([#894](https://github.com/m3db/m3/pull/894))
- **Query:** Add more PromQL functions (irate, temporal functions, etc) ([#872](https://github.com/m3db/m3/pull/872), [#897](https://github.com/m3db/m3/pull/897))
- **Coordinator:** Add downsampling to all configured namespaces by default ([#890](https://github.com/m3db/m3/pull/890))
- **DB:** By default use upsert behavior for datapoints written to mutable series ([#876](https://github.com/m3db/m3/pull/876))
- PERF **DB:** Various performance updates ([#889](https://github.com/m3db/m3/pull/889), [#903](https://github.com/m3db/m3/pull/903))

# 0.4.3 (2018-09-05)

- **Query:** Make compatible with Grafana Prometheus data source ([#871](https://github.com/m3db/m3/pull/871), [#877](https://github.com/m3db/m3/pull/877))
- FIX **DB:** Fix index correctness for multi-segment reads ([#883](https://github.com/m3db/m3/pull/883))
- PERF **DB:** Index performance improvements ([#880](https://github.com/m3db/m3/pull/880))

# 0.4.2 (2018-08-30)

- FIX **DB:** Remove native pooling and remove possibility of it being used by any components ([#870](https://github.com/m3db/m3/pull/870))
- FIX **DB:** Fix LRU series cache locking during very high throughput ([#862](https://github.com/m3db/m3/pull/862))

# 0.4.1 (2018-08-27)

- FIX **Coordinator:** Add support for negated match and regexp Prometheus remote read queries ([#858](https://github.com/m3db/m3/pull/858))

# 0.4.0 (2018-08-09)

- **Coordinator:** Add downsampling capabilities ([#796](https://github.com/m3db/m3/pull/796))
- **Query:** Add dedicated m3query service for serving PromQL and other supported query languages ([#817](https://github.com/m3db/m3/pull/817))
- **DB:** Add commit log snapshotting support to significantly reduce disk space usage by removing the majority of uncompressed commit logs on a database node and speed up bootstrap time ([#757](https://github.com/m3db/m3/pull/757)) ([#802](https://github.com/m3db/m3/pull/802))

- **Coordinator:** Add downsampling capabilities ([#796](https://github.com/m3db/m3/pull/796))
- **Query:** Add dedicated m3query service for serving PromQL and other supported query languages ([#817](https://github.com/m3db/m3/pull/817))
- **DB:** Add commit log snapshotting support to significantly reduce disk space usage by removing the majority of uncompressed commit logs on a database node and speed up bootstrap time ([#757](https://github.com/m3db/m3/pull/757)) ([#802](https://github.com/m3db/m3/pull/802))

# 0.3.0 (2018-07-19)

- **Coordinator:** Add m3coordinator Dockerfile and update Prometheus test to use standalone coordinator ([#792](https://github.com/m3db/m3/pull/792))
- **Coordinator:** Add rudimentary multi-cluster support for m3coordinator ([#785](https://github.com/m3db/m3/pull/785))
- **DB:** Fix documentation for single node walkthrough ([#795](https://github.com/m3db/m3/pull/795))

# 0.2.0 (2018-05-26)

- **Coordinator:** Build dedicated m3coordinator binary for release binaries

# 0.1.0 (2018-05-24)

- **DB:** Reverse indexing
- **Coordinator:** Prometheus remote read/write support
