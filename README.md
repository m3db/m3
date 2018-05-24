## WARNING: This is pre-release software, and is not intended for use until a stable release.

# M3DB [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fm3db%2Fm3db.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fm3db%2Fm3db?ref=badge_shield) [![Coverage Status](https://codecov.io/gh/m3db/m3db/branch/master/graph/badge.svg)](https://codecov.io/gh/m3db/m3db)

A time series database.

Documentation: https://m3db.github.io/m3db/

Notes for [developers]

[developers]: https://github.com/m3db/m3db/blob/master/DEVELOPER.md

## Test it out

(For a fully comprehsensive getting started guide, see our [single node how-to](https://m3db.github.io/m3db/how_to/single_node/)).

### Starting a node

```
# to build a local m3dbnode process
make m3dbnode

# run it with the sample configuration
./bin/m3dbnode -f ./src/dbnode/config/m3dbnode-local.yml
```

To cross-compile and build for Linux AMD64 build with `make m3dbnode-linux-amd64`.

### Test RPC

To test out some of the functionality of M3DB there are some user friendly HTTP JSON APIs that you can use.  These use the DB node cluster service endpoints.  

Note: performance sensitive users are expected to use the more performant endpoints via either the Go `src/dbnode/client/Session` API, or the GRPC endpoints exposed via `src/coordinator`.

#### Write a datapoint

```
curl http://localhost:9003/writetagged -s -X POST -d '{
  "namespace": "default",
  "id": "foo",
  "tags": [
    {
      "name": "city",
      "value": "new_york"
    },
    {
      "name": "endpoint",
      "value": "/request"
    }
  ],
  "datapoint": {
    "timestamp":'"$(date +"%s")"',
    "value": 42.123456789
  }
}'
```

#### Query for reverse indexed time series data

```
curl http://localhost:9003/query -s -X POST -d '{
  "namespace": "default",
  "query": {
    "regexp": {
      "field": "city",
      "regexp": ".*"
    }
  },
  "rangeStart": 0,
  "rangeEnd":'"$(date +"%s")"'
}' | jq .
```

## Building with Docker

A Dockerfile is included for both development and production deployment purposes. It uses a
[multi-stage build](https://docs.docker.com/develop/develop-images/multistage-build/) in order to
produce a lightweight production image from a single Dockerfile. Accordingly, it requires Docker
17.05 or later to build.

```
docker build -t m3dbnode:$(git rev-parse head) .
docker run --name m3dbnode m3dbnode:$(git rev-parse head)
```

If you wish to build an image with the source code included you can stop the build after the
`builder` stage:

```
docker build -t m3dbnode:$(git rev-parse head) --target builder .
```

## Configuration

The default Docker image will start a single `m3dbnode` process with an embedded etcd instance to
mimic a production environment. If you would like to further customize the configuration, you must
provide your own and mount it into the container:

```
docker run --name m3dbnode -v /host/config.yml:/etc/m3dbnode/myconfig.yml m3dbnode:tag -f /etc/m3dbnode/myconfig.yml
```

<hr>

This project is released under the [Apache License, Version 2.0](LICENSE).

[doc-img]: https://godoc.org/github.com/m3db/m3db?status.svg
[doc]: https://godoc.org/github.com/m3db/m3db
[ci-img]: https://semaphoreci.com/api/v1/m3db/m3db/branches/master/shields_badge.svg
[ci]: https://semaphoreci.com/m3db/m3db
