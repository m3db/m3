How to generate configs:

TODO: Set cluster var through std input

Config generation relies on the jsonnet package.

To setup:
```
cd genconfig
git submodule update
cd jsonnet
make
```


Example Usage:

Generate dtest config using the default GCP environment
```
./jsonnet/jsonnet templates/dtest-config.jsonnet
```

To generate a dtest config in YAML instead of JSON, you can run:

Note: Jsonnet adds an extra set of quotes around the output, this takes care of removing it.
Note: Jsonnet's output is a single string with escape characters, this will pretty print to a yaml file.
```
OUT="`./jsonnet/jsonnet templates/dtest-config.jsonnet`"; OUT=${OUT#?}; OUT=${OUT%?}; printf "$OUT" > out.yaml
```
