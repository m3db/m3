# clone_fileset

`clone_fileset` is a utility to make a copy of a specified fileset.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make clone_fileset
$ ./bin/clone_fileset -h

# example usage
# ./clone_fileset                        \
  -src-path-prefix /home/rungta          \
  -src-block-start 1494856800000000000   \
  -src-shard 3850 -src-namespace metrics \
  -dest-path-prefix /tmp/m3db-data-copy  \
  -dest-block-size 4h                    \
  -dest-block-start 1494867491000000     \
  -dest-shard 1024                       \
  -dest-namespace testmetrics            \
```

