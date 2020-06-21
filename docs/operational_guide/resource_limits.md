# Resource Limits and Preventing Abusive Reads/Writes

This operational guide provides an overview of how to set resource limits on 
M3 components to prevent abusive reads/writes impacting availability or 
performance of M3 in a production environment.

## M3DB

The best way to get started protecting M3DB nodes is to set a few limits on the
top level `limits` config stanza for M3DB.

When using M3DB for metrics workloads queries arrive as a set of matchers 
that select time series based on certain dimensions. The primary mechanism to 
protect against these matchers matching huge amounts of data in an unbounded 
way is to set a maximum limit for the amount of time series blocks allowed to
be matched and consequently read in a given time window. This can be done using `maxRecentlyQueriedSeriesBlocks` to set a maximum value and lookback time window 
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
