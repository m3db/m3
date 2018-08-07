# M3 [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status](https://codecov.io/gh/m3db/m3/branch/master/graph/badge.svg)](https://codecov.io/gh/m3db/m3) [![Gitter chat][gitter-img]](https://gitter.im/m3db/Lobby)

Distributed TSDB and Query Engine, Prometheus Sidecar, Metrics Aggregator, and more.

More information:
- [Documentation](documentation)
- [Developers](developers)

[documentation]: https://m3db.github.io/m3/
[developers]: https://github.com/m3db/m3/blob/master/DEVELOPER.md

## Test it out

(For a fully comprehsensive getting started guide, see our [single node how-to](https://m3db.github.io/m3/how_to/single_node/)).

### Starting a node

```
# to build a local m3dbnode process
make m3dbnode

# run it with the sample configuration
./bin/m3dbnode -f ./src/dbnode/config/m3dbnode-local-etcd.yml
```

To cross-compile and build for Linux AMD64 build with `make m3dbnode-linux-amd64`.

### Creating a namespace to store metrics

```
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "metrics",
  "retentionTime": "2h"
}'
```

### Test RPC

To test out some of the functionality of M3DB there are some user friendly HTTP JSON APIs that you can use.  These use the DB node cluster service endpoints.

Note: performance sensitive users are expected to use the more performant endpoints via either the Go `src/dbnode/client/Session` API, or the GRPC endpoints exposed via `src/coordinator`.

#### Write a datapoint

```
curl http://localhost:9003/writetagged -s -X POST -d '{
  "namespace": "metrics",
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
  "namespace": "metrics",
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

## Building the Docs

The `docs` folder contains our documentation in Markdown files. These Markdown files are built into a static site using
[`mkdocs`](https://www.mkdocs.org/) with the [`mkdocs-material`](https://squidfunk.github.io/mkdocs-material/) theme.
Building the docs using our predefined `make` targets requires a working Docker installation:

```
# generate the docs in the `site/` directory
make docs-build

# build docs and serve on localhost:8000 (with live reload)
make docs-serve

# build the docs and auto-push to the `gh-pages` branch
make docs-deploy
```

<hr>

This project is released under the [Apache License, Version 2.0](LICENSE).

[doc-img]: https://godoc.org/github.com/m3db/m3?status.svg
[doc]: https://godoc.org/github.com/m3db/m3
[ci-img]: https://semaphoreci.com/api/v1/m3db/m3/branches/master/shields_badge.svg
[ci]: https://semaphoreci.com/m3db/m3
[gitter-img]: https://badges.gitter.im/m3db.png
