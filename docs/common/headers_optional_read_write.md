
- `M3-Metrics-Type`:  
 If header is set it determines what type of metric to store this metric value as. By default metrics will be stored in all namespaces that are configured that don't have the `all` config set to `false` (by default this is set to `true` for all namespaces unless otherwise configured).  
 Must be one of:  
 `unaggregated`: Write metrics directly to configured unaggregated namespace.  
 `aggregated`: Write metrics directly to a configured aggregated namespace (bypassing any aggregation), this requires the `M3-Storage-Policy` header to be set to resolve which namespace to write metrics to.  

- `M3-Storage-Policy`:  
 If header is set it determines which aggregated namespace to write metrics directly to (bypassing any aggregation).  
 The value of the header must be in the format of `resolution:retention` in duration shorthand. e.g. `1m:48h` specifices 1 minute resolution and 48 hour retention. Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
