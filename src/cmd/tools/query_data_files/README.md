# query_data_files

`query_data_files` is a utility to calculate tiled sums on data for all timeseries' present in a TSDB file set.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make query_data_files
$ ./bin/query_data_files
Usage: query_data_files [-b value] [-t value] [-p value] [-s value] [parameters ...]
 -b, --block-start=value
       Block Start Time [in nsec]
 -t, --tile-size=value
       Tile size [in min]
 -n, --namespace=value
       Namespace [e.g. metrics]
 -p, --path-prefix=value
       Path prefix [e.g. /var/lib/m3db]
 -s, --shard=value
       Shard [expected format uint32]
 -c, --concurrency=value
       Concurrency [Concurrent iteration count, minimum 1, default is numCPUs]

# example usage
# query_data_files -b1480960800000000000 -n metrics -p /var/lib/m3db -s 451 -t 5 -c 3 > /tmp/sample-data.out
```

# TBH
- The tool outputs tile aggregations with non-zero values.
- The code currently assumes the data layout under the hood is `<path-prefix>/data/<namespace>/<shard>/...<block-start>-[index|...].db`. If this is not the file structure under the hood, replicate it to use this tool. Remember to copy checkpoint files along with each index file.
