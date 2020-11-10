---
title: "Mapping Rules"
weight: 13
---

Mapping rules are used to configure the storage policy for metrics. The storage policy
determines how long to store metrics for and at what resolution to keep them at.
For example, a storage policy of `1m:48h` tells M3 to keep the metrics for `48hrs` at a
`1min` resolution. Mapping rules can be configured in the `m3coordinator` configuration file
under the `downsample` > `rules` > `mappingRules` stanza. We will use the following as an
example. 

```yaml
downsample:
  rules:
    mappingRules:
      - name: "mysql metrics"
        filter: "app:mysql*"
        aggregations: ["Last"]
        storagePolicies:
          - resolution: 1m
            retention: 48h
      - name: "nginx metrics"
        filter: "app:nginx*"
        aggregations: ["Last"]
        storagePolicies:
          - resolution: 30s
            retention: 24h
          - resolution: 1m
            retention: 48h
```

Here, we have two mapping rules configured -- one for `mysql` metrics and one for `nginx`
metrics. The filter determines what metrics each rule applies to. The `mysql metrics` rule 
will apply to any metrics where the `app` tag contains `mysql*` as the value (`*` being a wildcard).
Similarly, the `nginx metrics` rule will apply to all metrics where the `app` tag contains 
`nginx*` as the value.

The `aggregations` field determines what functions to apply to the datapoints within a 
resolution tile. For example, if an application emits a metric every `10sec` and the resolution
for that metrics's storage policy is `1min`, M3 will need to combine 6 datapoints. If the `aggregations`
policy is `Last`, M3 will take the last value in that `1min` bucket. `aggregations` can be one 
of the following:

```
Last
Min
Max
Mean
Median
Count
Sum
SumSq
Stdev
P10
P20
P30
P40
P50
P60
P70
P80
P90
P95
P99
P999
P9999
```

Lastly, the `storagePolicies` field determines which namespaces to store the metrics in. For example, 
the `mysql` metrics will be sent to the `1m:48h` namespace, while the `nginx` metrics will be sent to 
both the `1m:48h` and `30s:24h` namespaces.

**Note:** the namespaces listed under the `storagePolicies` stanza must exist in M3DB.

## Rollup Rules

Rollup rules are used to rollup metrics and aggregate in different ways by 
arbitrary dimensions before they are stored. 

Here's an example of creating a new monotonic counter called 
`http_request_rollup_no_pod_bucket` from a set of histogram metrics originally 
called `http_request_bucket`:

```yaml
downsample:
  rules:
    rollupRules:
      - name: "http_request latency by route and git_sha without pod"
        filter: "__name__:http_request_bucket k8s_pod:* le:* git_sha:* route:*"
        transforms:
        - transform:
            type: "Increase"
        - rollup:
            metricName: "http_request_rollup_no_pod_bucket"
            groupBy: ["le", "git_sha", "route", "status_code", "region"]
            aggregations: ["Sum"]
        - transform:
            type: "Add"
        storagePolicies:
        - resolution: 30s
          retention: 720h
```

**Note:** only metrics that contain all of the `group_by` tags will be rolled up.
For example, in the above config, only `http_request_bucket` metrics that 
have all of the `group_by` labels present will be rolled up into the new
metric `http_request_rollup_no_pod_bucket`.

While the above example can be used to create a new rolled up metric, 
often times the goal of rollup rules is to eliminate the underlaying, 
raw metrics. In order to do this, a `mappingRule` will need to be 
added like in the following example (using the metric above as an example) 
with `drop` set to `true`. Additionally, if **all** of the underlaying metrics are
being dropped, there is no need to change the metric name (e.g. in the 
`rollupRule`, the `metricName` field can be equal to the existing metric) --
see below for an example.

```yaml
downsample:
  rules:
    mappingRules:
      - name: "http_request latency by route and git_sha drop raw"
        filter: "__name__:http_request_bucket k8s_pod:* le:* git_sha:* route:*"
        drop: true
    rollupRules:
      - name: "http_request latency by route and git_sha without pod"
        filter: "__name__:http_request_bucket k8s_pod:* le:* git_sha:* route:*"
        transforms:
        - transform:
            type: "Increase"
        - rollup:
            metricName: "http_request_bucket" # metric name doesn't change
            groupBy: ["le", "git_sha", "route", "status_code", "region"]
            aggregations: ["Sum"]
        - transform:
            type: "Add"
        storagePolicies:
        - resolution: 30s
          retention: 720h
```

**Note:** In order to store rolled up metrics in an `unaggregated` namespace, the namespace's `aggregationOptions` must have a matching `aggregation`. For example, if in the above rule, the `720h` namespace under `storagePolicies` 
is `unaggregated`, the `aggregationOptions` for that namespace should resemble the following:

```json
"aggregationOptions": {
  "aggregations": [
    {
      "aggregated": false
    },
    {
      "aggregated": true,
      "attributes": {
        "resolutionDuration": "30s",
        "downsampleOptions": { "all": false }
      }
    }
  ]
}
```
