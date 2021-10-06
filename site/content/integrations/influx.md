---
title: "InfluxDB"
weight: 5
---


This document is a getting started guide to integrating InfluxDB data pipelines 
with M3.

## Writing metrics using InfluxDB line protocol

To write metrics to M3 using the InfluxDB line protocol, simply form the request 
as you typically would line separated and POST the body to `/api/v1/influxdb/write` 
on the coordinator. Note that timestamp is in nanoseconds from Unix epoch.

This example writes two metrics `weather_temperature` and `weather_wind` using 
the current time in nanoseconds as the timestamp:
```shell
curl -i -X POST "{{% apiendpoint %}}influxdb/write" --data-binary "weather,location=us-midwest temperature=82,wind=42 $(expr $(date +%s) \* 1000000000)"
```

## Querying for metrics

After successfully written you can query for these metrics using PromQL. All 
measurements are translated into metric names by concatenating the name with
the measurement name.

The previous example forms the two following Prometheus time series:
```
weather_temperature{location="us-midwest"} 82
weather_wind{location="us-midwest"} 42
```

All metric names and labels are rewritten to contain only alphanumeric 
characters. Any non-alphanumeric characters are rewritten with an underscore.
