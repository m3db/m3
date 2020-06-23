# Resource Limits and Preventing Abusive Reads/Writes

This operational guide provides an overview of how to set resource limits on 
M3 components to prevent abusive reads/writes impacting availability or 
performance of M3 in a production environment.

## M3DB

### Configuring limits

The best way to get started protecting M3DB nodes is to set a few limits on the
top level `limits` config stanza for M3DB.

When using M3DB for metrics workloads, queries arrive as a set of matchers 
that select time series based on certain dimensions. The primary mechanism to 
protect against these matchers matching huge amounts of data in an unbounded 
way is to set a maximum limit for the amount of time series blocks allowed to
be matched and consequently read in a given time window. This can be done using 
`maxRecentlyQueriedSeriesBlocks` to set a maximum value and lookback time window 
to determine the duration over which the max limit is enforced.

You can use the Prometheus query `rate(query_stats_total_docs_per_block[1m])` to 
determine how many time series blocks are queried per second by your cluster 
today to determine what is a sane value to set this to. Make sure to multiply 
that number by the `lookback` period to get your desired max value. For 
instance, if the query shows that you frequently query 10,000 time series blocks 
per second safely with your deployment and you want to use the default lookback 
of `5s` then you would multiply 10,000 by 5 to get 50,000 as a max value with 
a 5s lookback.

### Annotated configuration

```
limits:
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
    lookback: 5s

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

```
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
    requireExhaustive: false

    # If set this limits the max number of datapoints allowed to be used by a
    # given query. This is applied at the query service after the result has 
    # been returned by a storage node.
    maxFetchedDatapoints: 0

  # If set will override default limits set globally.
  global:
    # If set this limits the max number of datapoints allowed to be used by all
    # queries at any point in time, this is applied at the query service after 
    # the result has been returned by a storage node.
    maxFetchedDatapoints: 0
```

### Headers

The following headers can also be used to override configured limits on a per request basis (to allow for different limits dependent on caller):

--8<--
docs/common/headers_optional_read_limits.md
--8<--
