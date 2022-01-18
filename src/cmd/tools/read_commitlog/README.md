# read_commitlog

`read_commitlog` is a utility to extract data from a commitlog file.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make read_commitlog
$ ./bin/read_commitlog
Usage: read_commitlog [-a value] [-f value] [-p value] [-s value] [-t value] [parameters ...]
 -a, --action=value
                   Action [print,summary]. Defaults to 'print'
 -f, --id-filter=value
                   ID Contains Filter (optional)
 -p, --path=value  file path [e.g.
                   /var/lib/m3db/commitlogs/commitlog-0-161023.db]
 -s, --id-size-filter=value
                   ID Size (bytes) Filter (optional)
 -t, --top=value   Print out only top N IDs

# Examples.

# get all datapoints for a given metric
$ read_commitlog -p /var/lib/m3db/commitlogs/commitlog-0-161023.db -f 'metric-name'

# get summary about commit log file
$ read_commitlog -p  /var/lib/m3db/commitlogs/commitlog-0-161023.db -a summary

# get summary about commit log file including top 100 largest and most frequent IDs 
$ read_commitlog -p  /var/lib/m3db/commitlogs/commitlog-0-161023.db -a summary -t 100

# get summary about commit log file including only IDs above 1000 bytes
$ read_commitlog -p  /var/lib/m3db/commitlogs/commitlog-0-161023.db -a summary -s 1000
```
