# split_shards

`split_shards` is a command line utility that traverses M3DB data folder
and splits data filesets into more granular shards.

# Usage

```
$ git clone git@github.com:m3db/m3.git
$ make split_shards
$ ./bin/split_shards
Usage: split_shards [-b value] [-d value] [-f value] [-h value] [-s value] [parameters ...]
 -b, --block-until=value
       Block Until Time, exclusive [in nsec]
 -d, --dst-path=value
       Destination path prefix [e.g. /var/lib/m3db/data]
 -f, --factor=value
       Integer factor to increase the number of shards by
 -h, --src-shards=value
       Original (source) number of shards
 -s, --src-path=value
       Source path [e.g. /temp/lib/m3db/data]
```
