# annotation_checker

`annotation_checker` is a utility that reads TSDB file sets and analyzes timeseries for missing initial annotations or annotation rewrites.

By default, this tools output the list of series IDs that have no annotation with the first datapoint in the block. If `--annotation-filter` is set to `annotation-rewritten`, the output will contain series IDs of metrics with rewritten annotations instead.

# Usage
```
$ make annotation_checker
$ ./bin/annotation_checker
Usage: annotation_checker [-P] [-a no-filtering|no-initial-annotation|annotation-rewritten] [-b nsec] [-f string] [-n string] [-p string] [-s int] [-t flush|snapshot] [-v int] [parameters ...]
 -a, --annotation-filter=no-filtering|no-initial-annotation|annotation-rewritten
                   Filters series by their annotations. Default:
                   no-initial-annotation
 -b, --block-start=nsec
                   Block Start Time
 -f, --id-filter=string
                   Filters series that contain given string in their IDs
 -n, --namespace=string
                   Namespace [e.g. metrics]
 -p, --path-prefix=string
                   Path prefix [e.g. /var/lib/m3db]
 -P, --print-annotations
                   Prints annotations
 -s, --shard=int   Shard number, or -1 for all shards in the directory
 -t, --fileset-type=flush|snapshot
                   Fileset type
 -v, --volume=int  Volume number

# example usage
# annotation_chekcer -p /var/lib/m3db -n metrics -s 451 -b 1480960800000000000
```

# TBH
- The tool outputs the identifiers to `stdout`, remember to redirect as desired.
- The code currently assumes the data layout under the hood is `<path-prefix>/data/<namespace>/<shard>/...<block-start>-[index|...].db`. If this is not the file structure under the hood, replicate it to use this tool. Remember to copy checkpoint files along with each index file.
