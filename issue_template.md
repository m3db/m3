# Filing M3 Issues

## General Issues

General issues are any non-performance related issues (data integrity, ease of use, error messages, configuration, documentation, etc).

Please provide the following information along with a description of the issue that you're experiencing:

1. What service is experiencing the issue? (M3Coordinator, M3DB, M3Aggregator, etc)
2. What is the configuration of the service? Please include any YAML files, as well as namespace / placement configuration (with any sensitive information anonymized if necessary).
3. How are you using the service? For example, are you performing read/writes to the service via Prometheus, or are you using a custom script?
4. Is there a reliable way to reproduce the behavior? If so, please provide detailed instructions.

## Performance issues

If the issue is performance related, please provide the following information along with a description of the issue that you're experiencing:

1. What service is experiencing the performance issue? (M3Coordinator, M3DB, M3Aggregator, etc)
2. Approximately how many datapoints per second is the service handling?
3. What is the approximate series cardinality that the series is handling in a given time window? I.E How many unique time series are being measured?
4. What is the hardware configuration (number CPU cores, amount of RAM, disk size and types, etc) that the service is running on? Is the service the only process running on the host or is it colocated with other software?
5. What is the configuration of the service? Please include any YAML files, as well as namespace / placement configuration (with any sensitive information anonymized if necessary).
6. How are you using the service? For example, are you performing read/writes to the service via Prometheus, or are you using a custom script?

In addition to the above information, CPU and heap profiles are always greatly appreciated.

### CPU / Heap Profiles

CPU and heap profiles are critical to helping us debug performance issues. All our services run with the [net/http/pprof](https://golang.org/pkg/net/http/pprof/) server enabled by default.

Instructions for obtaining CPU / heap profiles for various services are below, please attach these profiles to the issue whenever possible.

#### M3Coordinator

CPU
`curl <HOST_NAME>:<PORT(default 7201)>/debug/pprof/profile?seconds=5 > m3coord_cpu.out`

Heap
`curl <HOST_NAME>:<PORT(default 7201)>/debug/pprof/heap > m3coord_heap.out`

#### M3DB

CPU
`curl <HOST_NAME>:<PORT(default 9004)>/debug/pprof/profile?seconds=5 > m3db_cpu.out`

Heap
`curl <HOST_NAME>:<PORT(default 9004)>/debug/pprof/heap -> m3db_heap.out`

### M3DB Grafana Dashboard Screenshots

If the service experiencing performance issues is M3DB and you're monitoring it using Prometheus, any screenshots you could provide using [this](https://grafana.com/grafana/dashboards/8126-m3db-node-details/) dashboard would be helpful.