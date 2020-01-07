# verify_data_files

`verify_data_files` is a utility to verify data files in a given directory.

# Usage
```
$ git clone git@github.com:m3db/m3.git
$ make verify_data_files
$ ./bin/verify_data_files
Usage: verify_data_files [-fit] [-o value] [-p value] [parameters ...]
 -f, --fail-fast  Fail fast will bail on first failure
 -i, --fix-invalid-ids
                  Fix invalid IDs will remove entries with IDs that have
                  non-UTF8 chars
 -o, --fix-path-prefix=value
                  Fix output path file prefix for fixed files
 -p, --path-prefix=value
                  Path prefix [e.g. /var/lib/m3db]
 -t, --fix-invalid-tags
                  Fix invalid tags will remove tags with name/values non-UTF8
                  chars

# example usage
# verify_data_files --fail-fast --path-prefix /var/lib/m3db
```
