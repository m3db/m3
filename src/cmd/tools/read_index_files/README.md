# read_index_files

`read_index_files` is a utility to extract summary data for contents in an TSDB Index FileSet.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make read_index_files
$ ./bin/read_index_files
Usage: read_index_files [-b value] [-l value] [-n value] [-p value] [-v value] [parameters ...]
 -b, --block-start=value
       Block Start Time [in nsec]
 -l, --large-field-limit=value
       Large Field Limit (non-zero to display fields with num terms > limit)
 -n, --namespace=value
       Namespace [e.g. metrics]
 -p, --path-prefix=value
       Path prefix [e.g. /var/lib/m3db]
 -v, --volume-index=value
       Volume index

# example usage
# read_index_files -b1480960800000000000
```