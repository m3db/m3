# annotation_checker

`annotation_checker` is a utility that reads TSDB file sets and analyzes timeseries for missing initial annotations or annotation rewrites.

By default, this tools output the list of series IDs that have no annotation with the first datapoint in the block. If `--annotation-rewritten` flag is used, the output will contain series IDs of metrics with rewritten annotations instead.

# Usage
```
$ make annotation_checker
$ ./bin/annotation_checker
Usage: annotation_checker [-R] [-b value] [-n value] [-p value] [-s value] [-t value] [-v value] [parameters ...]
 -b, --block-start=value
                    Block Start Time [in nsec]
 -n, --namespace=value
                    Namespace [e.g. metrics]
 -p, --path-prefix=value
                    Path prefix [e.g. /var/lib/m3db]
 -R, --annotation-rewritten
                    Filters metrics with annotation rewrites
 -s, --shard=value  Shard number, or -1 for all shards in the directory
 -t, --fileset-type=value
                    flush|snapshot
 -v, --volume=value
                    Volume number

# example usage
# annotation_chekcer -p /var/lib/m3db -n metrics -s 451 -b 1480960800000000000
```

# TBH
- The tool outputs the identifiers to `stdout`, remember to redirect as desired.
- The code currently assumes the data layout under the hood is `<path-prefix>/data/<namespace>/<shard>/...<block-start>-[index|...].db`. If this is not the file structure under the hood, replicate it to use this tool. Remember to copy checkpoint files along with each index file.
