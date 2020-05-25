# M3 [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![FOSSA Status][fossa-img]][fossa] 

<p align="center"><img src="docs/theme/assets/images/M3-logo.png" alt="M3 Logo" width="256" height="270"></p>

[Distributed TSDB](http://m3db.github.io/m3/m3db/) and [Query Engine](http://m3db.github.io/m3/how_to/query/), [Prometheus Sidecar](http://m3db.github.io/m3/integrations/prometheus/), [Metrics Aggregator](http://m3db.github.io/m3/introduction/components/components/#m3-aggregator), and more. "More" now includes [Graphite storage and query engine](http://m3db.github.io/m3/integrations/graphite/)!

More information:

- [Documentation](https://m3db.github.io/m3/)
- [Developer: Getting Started](https://github.com/m3db/m3/blob/master/DEVELOPER.md)
- [Slack](http://bit.ly/m3slack)
- [Forum (Google Group)](https://groups.google.com/forum/#!forum/m3db)
- [Twitter](https://twitter.com/m3db_io)

## Community meetings

M3 contributors and maintainers have monthly (every four weeks) meetings. Join our M3 meetup group to receive notifications on upcoming meetings: 
[https://www.meetup.com/M3-Community/](https://www.meetup.com/M3-Community/).

## Test it out

The easiest way to testing out M3 is to follow one of the guides from the documentation. For a fully comprehensive getting started guide, see our [single node how-to](https://m3db.github.io/m3/how_to/single_node/).

### Starting a node

```
# to build a local m3dbnode process
make m3dbnode (note that we currently require at least Go 1.10 or higher)

# run it with the sample configuration
./bin/m3dbnode -f ./src/dbnode/config/m3dbnode-local-etcd.yml
```

To cross-compile and build for Linux AMD64 build with `make m3dbnode-linux-amd64`.

### Creating a namespace to store metrics

```
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "2h"
}'
```

### Test RPC

To test out some of the functionality of M3DB there are some user friendly HTTP JSON APIs that you can use. These use the DB node cluster service endpoints.

Note: performance sensitive users are expected to use the more performant endpoints via either the Go `src/dbnode/client/Session` API, or the GRPC endpoints exposed via `src/coordinator`.

#### Write a datapoint

```
curl http://localhost:9003/writetagged -s -X POST -d '{
  "namespace": "default",
  "id": "foo",
  "tags": [
    {
      "name": "__name__",
      "value": "user_login"
    },
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
docker build -f docker/m3dbnode/Dockerfile -t m3dbnode:$(git rev-parse head) .
docker run --name m3dbnode m3dbnode:$(git rev-parse head)
```

If you wish to build an image with the source code included you can stop the build after the
`builder` stage:

```
docker build -f docker/m3dbnode/Dockerfile -t m3dbnode:$(git rev-parse head) --target builder .
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

[doc-img]: https://godoc.org/github.com/m3db/m3?status.svg
[doc]: https://godoc.org/github.com/m3db/m3
[ci-img]: https://badge.buildkite.com/5509d9360bfea7f99ac3a07fd029feb1aafa5cff9ed5ab667b.svg?branch=master
[ci]: https://buildkite.com/uberopensource/m3-monorepo-ci
[cov-img]: https://codecov.io/gh/m3db/m3/branch/master/graph/badge.svg
[cov]: https://codecov.io/gh/m3db/m3
[fossa-img]: https://app.fossa.io/api/projects/custom%2B4529%2Fgithub.com%2Fm3db%2Fm3.svg?type=shield
[fossa]: https://app.fossa.io/projects/custom%2B4529%2Fgithub.com%2Fm3db%2Fm3?ref=badge_shield