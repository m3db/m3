# read_commitlog

`read_commitlog` is a utility to extract data from a commitlog file.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make read_commitlog
$ ./bin/read_commitlog
Usage: read_commitlog [-p value] [-f value]
 -p, --path=value
       Commitlog file path [e.g. /var/lib/m3db/commitlogs/commitlog-0-161023.db]
 -f, --id-filter=value
       ID Contains Filter [e.g. xyz]

# example usage
# read_commitlog -p /var/lib/m3db/commitlogs/commitlog-0-161023.db -f 'metric-name' > /tmp/sample-data.out
```
