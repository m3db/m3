---
title: "API"
menuTitle: "Query"
weight: 1
---


**Please note:** This documentation is a work in progress and more detail is required.

## Query using PromQL

Query using PromQL and returns JSON datapoints compatible with the Prometheus Grafana plugin.

### URL

`/api/v1/query_range`

### Method

`GET`

### URL Params

#### Required

- `start=[time in RFC3339Nano]`
- `end=[time in RFC3339Nano]`
- `step=[time duration]`
- `target=[string]`

#### Optional

- `debug=[bool]`
- `lookback=[string|time duration]`: This sets the per request lookback duration to something other than the default set in config, can either be a time duration or the string "step" which sets the lookback to the same as the `step` request parameter.

### Header Params

#### Optional

{{% fileinclude "headers_optional_read_write_all.md" %}}

{{% fileinclude "headers_optional_read_all.md" %}}

### Data Params

None.

### Sample Call

<!-- 
Note: keep this example similar to the one found in coordinator API 
documentation for consistency/ease of readers.
-->
```shell
curl 'http://localhost:7201/api/v1/query_range?query=abs(http_requests_total)&start=1530220860&end=1530220900&step=15s'
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "code": "200",
          "handler": "graph",
          "method": "get"
        },
        "values": [
          [
            1530220860,
            "6"
          ],
          [
            1530220875,
            "6"
          ],
          [
            1530220890,
            "6"
          ]
        ]
      },
      {
        "metric": {
          "code": "200",
          "handler": "label_values",
          "method": "get"
        },
        "values": [
          [
            1530220860,
            "6"
          ],
          [
            1530220875,
            "6"
          ],
          [
            1530220890,
            "6"
          ]
        ]
      }
    ]
  }
}
```
