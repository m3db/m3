# read_segments

`read_segments` is a utility to read segments in a given directory.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make read_segments
$ ./bin/read_segments
Usage: read_index_segments [-o value] [-p value] [parameters ...]
 -o, --output-file=value
       Output JSON file of line delimited JSON objects for each
       segment
 -p, --path-prefix=value
       Path prefix [e.g. /var/lib/m3db]
```
