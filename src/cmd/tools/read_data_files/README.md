# read_data_files

`read_data_files` is a utility to extract data for all timeseries' present in a TSDB file set.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make read_data_files
$ ./bin/read_data_files
Usage: read_data_files [-NR] [-B value] [-b value] [-f value] [-n value] [-p value] [-s value] [-t value] [-v value] [parameters ...]
 -B, --benchmark=value
                    benchmark mode (optional), [series|datapoints]
 -b, --block-start=value
                    Block Start Time [in nsec]
 -f, --id-filter=value
                    ID Contains Filter (optional)
 -n, --namespace=value
                    Namespace [e.g. metrics]
 -N, --no-initial-annotation
                    Filters metrics that have no annotation in first datapoint
                    of the block and prints their series IDs
 -p, --path-prefix=value
                    Path prefix [e.g. /var/lib/m3db]
 -R, --annotation-rewritten
                    Filters metrics with annotation rewrites and prints their
                    series IDs
 -s, --shard=value  Shard [expected format uint32]
 -t, --fileset-type=value
                    flush|snapshot
 -v, --volume=value
                    Volume number

# example usage
# read_data_files -b1480960800000000000 -n metrics -p /var/lib/m3db -s 451 -f 'metric-name' > /tmp/sample-data.out
```

# TBH
- The tool outputs the identifiers to `stdout`, remember to redirect as desired.
- The code currently assumes the data layout under the hood is `<path-prefix>/data/<namespace>/<shard>/...<block-start>-[index|...].db`. If this is not the file structure under the hood, replicate it to use this tool. Remember to copy checkpoint files along with each index file.
