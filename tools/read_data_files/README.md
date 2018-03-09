# read_data_files

`read_data_files` is a utility to extract data for all timeseries' present in a TSDB file set.

# Usage
```
$ git clone git@github.com:m3db/m3db.git
$ make read_data_files
$ ./bin/read_data_files
Usage: read_data_files [-b value] [-n value] [-p value] [-s value] [parameters ...]
 -b, --block-start=value
       Block Start Time [in nsec]
 -f, --id-filter=value
       ID Contains Filter [e.g. xyz]
 -n, --namespace=value
       Namespace [e.g. metrics]
 -p, --path-prefix=value
       Path prefix [e.g. /var/lib/m3db]
 -s, --shard=value
       Shard [expected format uint32]

# example usage
# read_data_files -b1480960800000000000 -n metrics -p /var/lib/m3db -s 451 -f 'metric-name' > /tmp/sample-data.out
```

# TBH
- The tool outputs the identifiers to `stdout`, remember to redirect as desired.
- The code currently assumes the data layout under the hood is `<path-prefix>/data/<namespace>/<shard>/...<block-start>-[index|...].db`. If this is not the file structure under the hood, replicate it to use this tool. Remember to copy checkpoint files along with each index file.