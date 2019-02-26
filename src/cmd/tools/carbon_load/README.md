# carbon_load

`carbon_load` is a tool to generate load on a carbon ingestion port.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make carbon_load
$ ./bin/clone_fileset -h

# example usage
# ./carbon_load               \
  -target="0.0.0.0:7204"      \
  -numWorkers="20"            \
  -cardinality=1000           \
  -name="local.random"        \
  -qps=1000                   \
  -duration="30s"
```

# Benchmarking Carbon Ingestion

The easiest way to benchmark carbon ingestion end-to-end is to:

1. Use `start_m3` to get an entire M3 stack running, including an aggregated namespace and a carbon ingestion port.

2. Run this tool with the target pointed at the exposed carbon ingestion port on the `start_m3` stack.

3. Use `pprof` to take CPU and heap profiles of `m3coordinator` while the `carbon_load` tool is running.
