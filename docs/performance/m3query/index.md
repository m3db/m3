## Performance configurations

Below are some common configurations related to performance for the query engine.

### Query conversion LRU

Before fetching data from M3DB, all queries are converted to a format that M3DB can understand. We have an LRU cache that caches the results of this conversion so that queries that are the same do not need to go through the conversion process each time. By default, the size of the cache is set to `4096`, however, this number is configurable:

```yaml
cache:
  queryConversion:
    size: <int>
```
