---
title: "Resource Limits and Preventing Abusive Reads/Writes"
weight: 4
---

This operational guide provides an overview of how to set resource limits on 
M3 components to prevent abusive reads/writes impacting availability or 
performance of M3 in a production environment.

## M3DB

### Configuring limits

The best way to get started protecting M3DB nodes is to set a few resource limits on the
top level `limits` config stanza for M3DB.

The primary limit is on total bytes recently read from disk across all queries
since this most directly causes memory pressure. Reading time series data that 
is already in-memory (either due to already being cached or being actively written) 
costs much less than reading historical time series data which must be read from disk. 
By specifically limiting bytes read from disk, and excluding bytes already in-memory, we
can apply a limit that most accurately reflects increased memory pressure on the database nodes.
To set a limit, use the `maxRecentlyQueriedSeriesDiskBytesRead` stanza to define a 
policy for how much historical time series data can be read over a given 
lookback time window. The `value` specifies max numbers of bytes read from disk allowed
within a given `lookback` period.

You can use the Prometheus query `rate(query_limit_total_disk_bytes_read[1m])` to determine 
how many bytes are read from disk per second by your cluster today to inform an appropriate limit.
Make sure to multiply that number by the `lookback` period to get your desired max value. For 
instance, if the query shows that you frequently read 100MB
per second safely with your deployment and you want to use the default lookback 
of `15s` then you would multiply 100MB by 15 to get 1.5GB as a max value with 
a 15s lookback.

The secondary limit is on the total volume of time series data recently read 
across all queries (in-memory or not), since even querying data already in memory in an unbounded 
manner can overwhelm a database node. When using M3DB for metrics workloads, 
queries arrive as a set of matchers that select time series based on certain 
dimensions. The primary mechanism to protect against these matchers matching 
huge amounts of data in an unbounded way is to set a maximum limit for the 
amount of time series blocks allowed to be matched and consequently read in a 
given time window. Use the `maxRecentlyQueriedSeriesBlocks` to 
set a maximum `value` and `lookback` time window to determine the duration over 
which the max limit is enforced.

You can use the Prometheus query `rate(query_limit_total_docs_matched[1m])` to 
determine how many time series blocks are queried per second by your cluster 
today to inform and appripriate limit. Make sure to multiply 
that number by the `lookback` period to get your desired max value. For 
instance, if the query shows that you frequently query 10,000 time series blocks 
per second safely with your deployment and you want to use the default lookback 
of `15s` then you would multiply 10,000 by 15 to get 150,000 as a max value with 
a 15s lookback.

The third limit `maxRecentlyQueriedSeriesDiskRead` caps the series IDs matched by incoming 
queries. This originally was distinct from the limit `maxRecentlyQueriedSeriesBlocks`, which
also limits the memory cost of specific series matched, because of an inefficiency
in how allocations would occur even for series known to not be present on disk for a given
shard. This inefficiency has been resolved https://github.com/m3db/m3/pull/3103 and therefore
this limit should be tracking memory cost linearly relative to `maxRecentlyQueriedSeriesBlocks`.
It is recommended to defer to using `maxRecentlyQueriedSeriesBlocks` over 
`maxRecentlyQueriedSeriesDiskRead` given both should cap the resources similarly.

### Annotated configuration

```yaml
limits:
  # If set, will enforce a maximum cap on disk read bytes for time series that
  # resides historically on disk (and are not already in memory).
  maxRecentlyQueriedSeriesDiskBytesRead:
    # Value sets the maximum disk bytes read for historical data.
    value: 0
    # Lookback sets the time window that this limit is enforced over, every 
    # lookback period the global count is reset to zero and when the limit 
    # is reached it will reject any further time series blocks being matched 
    # and read until the lookback period resets.
    lookback: 15s

  # If set, will enforce a maximum cap on time series blocks matched for
  # queries searching time series by dimensions.
  maxRecentlyQueriedSeriesBlocks:
    # Value sets the maximum time series blocks matched, use your block 
    # settings to understand how many datapoints that may actually translate 
    # to (e.g. 2 hour blocks for unaggregated data with 30s scrape interval
    # will translate to 240 datapoints per single time series block matched).
    value: 0
    # Lookback sets the time window that this limit is enforced over, every 
    # lookback period the global count is reset to zero and when the limit 
    # is reached it will reject any further time series blocks being matched 
    # and read until the lookback period resets.
    lookback: 15s

  # If set, will enforce a maximum on the series read from disk.
  # This limit can be used to ensure queries that match an extremely high 
  # volume of series can be limited before even reading the underlying series data from disk.
  maxRecentlyQueriedSeriesDiskRead:
    # Value sets the maximum number of series read from disk.
    value: 0
    # Lookback sets the time window that this limit is enforced over, every 
    # lookback period the global count is reset to zero and when the limit 
    # is reached it will reject any further time series blocks being matched 
    # and read until the lookback period resets.
    lookback: 15s

  # If set then will limit the number of parallel write batch requests to the 
  # database and return errors if hit.
  maxOutstandingWriteRequests: 0

  # If set then will limit the number of parallel read requests to the 
  # database and return errors if hit. 
  # Note since reads can be so variable in terms of how expensive they are
  # it is not always very useful to use this config to prevent resource 
  # exhaustion from reads.
  maxOutstandingReadRequests: 0
```

### Dynamic configuration

Query limits can be dynamically driven by etcd to adjust limits without redeploying. By updating the `m3db.query.limits` key in etcd, specific limits can be overriden. M3Coordinator exposes an API for updating etcd key/value pairs and so this API can be used for modifying these dynamic overrides. For example,

```
curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/kvstore -d '{
  "key": "m3db.query.limits",
  "value":{
    "maxRecentlyQueriedSeriesDiskBytesRead": {
      "limit":0,
      "lookbackSeconds":15,
      "forceExceeded":false
    },
    "maxRecentlyQueriedSeriesBlocks": {
      "limit":0,
      "lookbackSeconds":15,
      "forceExceeded":false
    },
    "maxRecentlyQueriedSeriesDiskRead": {
      "limit":0,
      "lookbackSeconds":15,
      "forceExceeded":false
    }
  },
  "commit":true
}'
```

To remove all overrides, omit all limits from the `value`
```
curl -vvvsSf -X POST 0.0.0.0:7201/api/v1/kvstore -d '{
  "key": "m3db.query.limits",
  "value":{},
  "commit":true
}'
```

Usage notes:
- Setting the `commit` flag to false allows for dry-run API calls to see the old and new limits that would be applied.
- Omitting a limit from the `value` results in that limit to be driven by the config-based settings.
- The `forceExceeded` flag makes the limit behave as though it is permanently exceeded, thus failing all queries. This is useful for dynamically shutting down all queries in cases where load may be exceeding provisioned resources.

## M3 Query and M3 Coordinator

### Deployment

Protecting queries impacting your ingestion of metrics for metrics workloads 
can first and foremost be done by deploying M3 Query and M3 Coordinator 
independently. That is, for writes to M3 use a dedicated deployment of 
M3 Coordinator instances, and then for queries to M3 use a dedicated deployment 
of M3 Query instances.

This ensures when M3 Query instances become busy and are starved of resources 
serving an unexpected query load, they will not interrupt the flow of metrics
being ingested to M3.

### Configuring limits

To protect against individual queries using too many resources, you can specify some
sane limits in the M3 Query (and consequently M3 Coordinator) configuration 
file under the top level `limits` config stanza.

There are two types of limits:

- Per query time series limit
- Per query time series * blocks limit (docs limit)

When either of these limits are hit, you can define the behavior you would like, 
either to return an error when this limit is hit, or to return a partial result 
with the response header `M3-Results-Limited` detailing the limit that was hit 
and a warning included in the response body.

### Annotated configuration

```yaml
limits:
  # If set will override default limits set per query.
  perQuery:
    # If set limits the number of time series returned for any given 
    # individual storage node per query, before returning result to query 
    # service.
    maxFetchedSeries: 0

    # If set limits the number of index documents matched for any given 
    # individual storage node per query, before returning result to query 
    # service.
    # This equates to the number of time series * number of blocks, so for 
    # 100 time series matching 4 hours of data for a namespace using a 2 hour 
    # block size, that would result in matching 200 index documents.
    maxFetchedDocs: 0

    # If true this results in causing a query error if the query exceeds 
    # the series or blocks limit for any given individual storage node per query.
    requireExhaustive: true
```

### Headers

The following headers can also be used to override configured limits on a per request basis (to allow for different limits dependent on caller):


{{% fileinclude file="headers_optional_read_limits.md" %}}