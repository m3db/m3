# M3 [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![FOSSA Status][fossa-img]][fossa] 

<p align="center"><img src="site/static/images/logo-square.png" alt="M3 Logo" width="256" height="270"></p>

[Distributed TSDB](https://docs.m3db.io/m3db/) and [Query Engine](https://docs.m3db.io/how_to/query/), [Prometheus Sidecar](https://docs.m3db.io/integrations/prometheus/), [Metrics Aggregator](https://docs.m3db.io/overview/components/#m3-aggregator), and more such as [Graphite storage and query engine](https://docs.m3db.io/integrations/graphite/).

## Table of Contents:
- [Install](#install)
- [Dependencies](#dependencies)
- [Usage](#usage)
- [Contributing](#contributing)
- [Community meetings](#community-meetings)
- [Office Hours](#office-hours)

## More information

- [Documentation](https://docs.m3db.io/)
- [Developer Guide: Getting Started](https://github.com/m3db/m3/blob/master/DEVELOPMENT.md)
- [Slack](http://bit.ly/m3slack)
- [Forum (Google Group)](https://groups.google.com/forum/#!forum/m3db)

## Install 



### Dependencies 

The simplest and quickest way to try M3 is to use Docker, read [the M3 quickstart section](https://m3db.io/docs/quickstart) for other options. 

```

## Usage 

The below is a simplified version of the [M3 quickstart guide](https://m3db.io/docs/quickstart/docker/), and we suggest you read that for more details.

1. Start a container
```shell
docker run -p 7201:7201 -p 7203:7203 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db quay.io/m3db/m3dbnode:v1.0.0
```

2. Create a placement and namespace
```shell
#!/bin/bash
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "12h"
}' | jq .
```

3. Ready a namespace
```shell
curl -X POST http://localhost:7201/api/v1/services/m3db/namespace/ready -d '{
  "name": "default"
}' | jq .
```

4. Write metrics
```shell
#!/bin/bash
curl -X POST http://localhost:7201/api/v1/json/write -d '{
  "tags": 
    {
      "__name__": "third_avenue",
      "city": "new_york",
      "checkout": "1"
    },
    "timestamp": '\"$(date "+%s")\"',
    "value": 3347.26
}'
```

5. Query results
**Linux**
```shell
curl -X "POST" -G "http://localhost:7201/api/v1/query_range" \
  -d "query=third_avenue" \
  -d "start=$(date "+%s" -d "45 seconds ago")" \
  -d "end=$( date +%s )" \
  -d "step=5s" | jq .  
```

## Contributing 

You can ask questions and give feedback in the following ways:
- [Create a GitHub issue](https://github.com/m3db/m3/issues)
- [In the public M3 Slack](http://bit.ly/m3slack)
- [In the M3 forum (Google Group)](https://groups.google.com/forum/#!forum/m3db)

M3 welcomes pull requests…
Read [the development guide](DEVELOPMENT.md) to help you get setup for building and contributing to M3.

## Community Meetings

M3 contributors and maintainers have monthly (every four weeks) meetings. Join our M3 meetup group to receive notifications on upcoming meetings: 
[https://www.meetup.com/M3-Community/](https://www.meetup.com/M3-Community/).

Recordings of past meetups can be found here: [https://vimeo.com/user/120001164/folder/2290331](https://vimeo.com/user/120001164/folder/2290331).

## Office Hours

Members of the M3 team will hold office hours on the third Thursday of every month from 11-1pm EST. In order to join, make sure to sign up for a slot here: [https://calendly.com/chronosphere-intro/m3-community-office-hours](https://calendly.com/chronosphere-intro/m3-community-office-hours).

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
