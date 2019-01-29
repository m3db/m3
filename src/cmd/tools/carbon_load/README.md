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

