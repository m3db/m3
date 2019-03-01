## Performance configurations

Below are some common configurations related to performance for the query engine.

### Query conversion LRU

Before fetching data from M3DB, all queries are converted to a format that M3DB can understand. Considering that the majority of these queries are repeated
(dashboards and alerts being fairly fixed), the converted results are cached -- up to a default max of `4096`. To fine tune the size of the cache, the following configuration settings can be added:

```yaml
cache:
  queryConversion:
    size: <int>
```

**Note:** We recommend changing the size of the cache to be roughly the number of queries being executed (including both dashboards and alerts).
Setting the size to `0` will turn this cache off.
