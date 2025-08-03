# Protobuf Client Example

1. Setup an M3DB container as described in the [using M3DB as a general purpose time series database guide](https://docs.m3db.io/how_to/use_as_tsdb).
2. Modify `config.yaml` with any changes you've made to the default configuration. Also, if you make any changes to M3DB's configuration, make sure to do so before restarting the container as M3DB does not reload YAML configuration dynamically.
3. Execute `go run main.go -f config.yaml`