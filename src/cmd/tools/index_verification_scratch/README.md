
# index_verification_scratch

`index_verification_scratch` is a utility to try several potential index verification methods against a saved data set.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make query_data_files
$ ./bin/query_data_files
Usage: query_data_files [-b value] [-t value] [-p value] [-s value] [parameters ...]
 -b, --block-start=value
       Block Start Time [in nsec]
 -f, --fileset-type=value
       flush|snapshot
 -n, --namespace=value
       Namespace [e.g. metrics]
 -p, --path-prefix=value
       Path prefix [e.g. /var/lib/m3db]
 -v, --volume=value
       Volume number [e.g. 0]
 -u    use vellum
 -s    use snappy (this only works with vellum)

# example usage
# query_data_files -b 1596009600000000000 -n default -v 0 -p /var/lib/m3db
```

# TBH
- The tool outputs tile aggregations with non-zero values.
- The code currently assumes the data layout under the hood is `<path-prefix>/data/<namespace>/<shard>/...<block-start>-[index|...].db`. If this is not the file structure under the hood, replicate it to use this tool. Remember to copy checkpoint files along with each index file.
