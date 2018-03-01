# verify_commitlogs

`verify_commitlogs` is a utility to verify a set of commit logs to ensure they are valid. It's also useful for testing / benchmarking the commitlog bootstrapper. Note that it requires the commitlogs to be present in a folder called "commitlogs" inside of the directory provided as the -path-prefix argument.

# Usage

```bash
$ git clone git@github.com:m3db/m3db.git
$ make verify_commitlogs
$ ./bin/verify_commitlogs -h
```

# Example usage
```bash
./verify_commitlogs     \
   -path-prefix /var/lib/m3db \
   -namespace metrics         \
   -block-size 2h             \
   -start-unix-timestamp 1507667028 \
   -end-unix-timestamp 1507677000 \
   -debug-listen-address :8080 \
```

Some of the flags in the example above are optional, and there are other flags
that are not shown. Run `./verify_commitlogs -h` to see all possible flags.