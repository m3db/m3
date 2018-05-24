How to generate configs:

TODO: Set cluster var through std input

Config generation relies on the jsonnet package.


Example Usage:

Generate dtest config using the GCP environment
```
cd GOPATH/src/m3db/m3db
CLUSTER=gcp make dtest-config
```
