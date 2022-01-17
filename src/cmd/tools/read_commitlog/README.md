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

# Examples.

# get all datapoints for a given metric
$ read_commitlog -p /var/lib/m3db/commitlogs/commitlog-0-161023.db -f 'metric-name' > /tmp/sample-data.out

# get summary about commit log file
$ read_commitlog -p ~/Code/commitlogs/square/commitlog-0-45740.db -a summary > /tmp/sample-data.out

# get summary about commit log file including top 100 largest and most frequent IDs 
$ read_commitlog -p ~/Code/commitlogs/square/commitlog-0-45740.db -a summary -t 100 > /tmp/sample-data.out

# get summary about commit log file including only IDs above 1000 bytes
$ read_commitlog -p ~/Code/commitlogs/square/commitlog-0-45740.db -a summary -s 1000 > /tmp/sample-data.out
```
