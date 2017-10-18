# bootstrap_commitlogs

`bootstrap_commitlogs` is a utility to bootstrap a set of commit logs to ensure they are valid. It's also useful for testing / benchmarking the commitlog bootstrapper.

# Usage

```bash
$ git clone git@github.com:m3db/m3db.git
$ make bootstrap_commitlogs
$ ./bin/bootstrap_commitlogs -h
```

# Example usage
```bash
./verify_commitlogs     \
   -path-prefix /var/lib/m3db \
   -namespace metrics         \
   -block-size 2h             \
   -start-unix-timestamp 1507667028 \
   -end-unix-timestamp 1507677000 \
```