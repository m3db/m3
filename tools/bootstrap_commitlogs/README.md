# bootstrap_commitlogs

`bootstrap_commitlogs` is a utility to bootstrap a set of commit logs to ensure they are valid.

# Usage
```
$ git clone git@github.com:m3db/m3db.git
$ make bootstrap_commitlogs
$ ./bin/bootstrap_commitlogs -h

# example usage
# ./bootstrap_commitlogs     \
  -path-prefix /var/lib/m3db \
  -namespace metrics         \
  -block-size 2h             \
```
