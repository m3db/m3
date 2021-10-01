# split_idx_shards

`split_idx_shards` is a command line utility that traverses M3DB index folder
and updates reverse index filesets to contain the ids of shards after shard splitting
(see [split_shards](../split_shards)).

# Usage

```
$ git clone git@github.com:m3db/m3.git
$ make split_idx_shards
$ ./bin/split_idx_shards
Usage: split_idx_shards [-b value] [-f value] [-h value] [-p value] [parameters ...]
 -b, --block-until=value
                   Block Until Time, exclusive [in nsec]
 -f, --factor=value
                   Integer factor to increase the number of shards by
 -h, --src-shards=value
                   Original (source) number of shards
 -p, --path=value  Index path [e.g. /temp/lib/m3db/index]
```
