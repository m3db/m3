# read_index_ids

`read_index_ids` is a utility to extract identifiers for all timeseries' present in a TSDB file set.

# Usage
```
$ git clone git@github.com:m3db/m3db.git
$ make read_index_ids
$ ./bin/read_index_ids
Usage: read_index_ids [-b value] [-n value] [-p value] [-s value] [parameters ...]
 -b, --block-start=value
       Block Start Time [in nsec]
 -n, --namespace=value
       Namespace [e.g. metrics]
 -p, --path-prefix=value
       Path prefix [e.g. /var/lib/m3db]
 -s, --shard=value
       Shard [expected format uint32]

# example usage
# read_index_ids -b1480960800000000000 -n metrics -p /var/lib/m3db -s 451 > /tmp/sample-index.out
```

# TBH
- The tool outputs the identifiers to `stdout`, remember to redirect as desired.
- The code currently assumes the data layout under the hood is `<path-prefix>/data/<namespace>/<shard>/...<block-start>-[index|...].db`. If this is not the file structure under the hood, replicate it to use this tool. Remember to copy checkpoint files along with each index file.