## WARNING: This is pre-release software, and is not intended for use until a stable release.

# M3DB [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status](https://codecov.io/gh/m3db/m3db/branch/master/graph/badge.svg)](https://codecov.io/gh/m3db/m3db)

A time series database.


Documentation: https://m3db.github.io/m3db/

Notes for [developers]

[developers]: https://github.com/m3db/m3db/blob/master/DEVELOPER.md

## Test it out

### Starting a node

To start a local node, you can build with `make m3dbnode` and then run `./bin/m3dbnode -f ./example/m3db-node-config.yaml`.

### RPC

To test out some of the functionality of M3DB there are some user friendly HTTP JSON APIs that you can use.

#### Write a datapoint

```
curl http://localhost:9000/writetagged -X POST -d '{"namespace":"metrics","id":"foo","tags":[{"name":"city","value":"new_york"},{"name":"endpoint","uri":"/request"}],"datapoint":{"timestamp":152653946,"value":42.2}}'
```

#### Query for indexed time series data

```
curl -s http://localhost:9000/query -X POST -d '{"namespace":"metrics","rangeStart":1526529696,"rangeEnd":1526529699,"query":{"regexp":{"field":"city","regexp":"^new_.*r.*$"}}}'
```

<hr>

This project is released under the [MIT License](LICENSE.md).

[doc-img]: https://godoc.org/github.com/m3db/m3db?status.svg
[doc]: https://godoc.org/github.com/m3db/m3db
[ci-img]: https://semaphoreci.com/api/v1/m3db/m3db/branches/master/shields_badge.svg
[ci]: https://semaphoreci.com/m3db/m3db
