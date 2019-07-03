- `M3-Metrics-Type`:  
 If header is set it determines what type of metric to store this metric value as. Otherwise by default metrics will be stored in all namespaces that are configured. You can also disable this default behavior by setting `downsample` options to `all: false` for a namespace in the coordinator config, for more see [disabling automatic aggregation](/how_to/query.md#disabling-automatic-aggregation).<br /><br />
 Must be one of:  
 `unaggregated`: Write metrics directly to configured unaggregated namespace.  
 `aggregated`: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the `M3-Storage-Policy` header to be set to resolve which namespace to write metrics to.  
<br />
- `M3-Storage-Policy`:  
 If header is set it determines which aggregated namespace to write metrics directly to (bypassing any aggregation).  
 The value of the header must be in the format of `resolution:retention` in duration shorthand. e.g. `1m:48h` specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
