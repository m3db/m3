*   `M3-Metrics-Type`:  
    If this header is set, it determines what type of metric to store this metric value as. Otherwise by default, metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting `downsample` options to `all: false` for a namespace in the coordinator config, for more see [disabling automatic aggregation](/docs/how_to/m3query.md#disabling-automatic-aggregation).

     Must be one of:  
    `unaggregated`: Write metrics directly to configured unaggregated namespace.  
    `aggregated`: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the `M3-Storage-Policy` header to be set to resolve which namespace to write metrics to.  

*   `M3-Storage-Policy`:  
     If this header is set, it determines which aggregated namespace to read/write metrics directly to/from (bypassing any aggregation).  
     The value of the header must be in the format of `resolution:retention` in duration shorthand. e.g. `1m:48h` specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".<br /><br />
    Here is [an example](https://github.com/m3db/m3/blob/master/scripts/docker-integration-tests/prometheus/test.sh#L126-L146) of querying metrics from a specific namespace. 
