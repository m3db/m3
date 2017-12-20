## WARNING: This is Alpha software and not intended for use until a stable release.

# M3Coordinator [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]

Service to access M3DB

### Running in Docker

> Note that all commands are run within the root of the m3coordinator directory except where specified.

**Running both m3coordinator and Prometheus in containers:**

You can launch a Prometheus and m3coordinator container using `docker-compose`. However, you must first build the m3coordinator Docker image.

To do so, you will need the m3coordinator binary:

    $ GOOS=linux GOARCH=amd64 CGO_ENABLED=0 make services

Once you have the binary, you can run the following to make the Docker image:

    $ docker build -t m3coordinator -f docker/Dockerfile .

Finally, you can spin up the two containers using `docker-compose` within the `docker/` directory:

    $ docker-compose up

> Note: The default local ports for Prometheus and m3coordinator are `9090` and `1234`, respectively, and the default `prometheus.yml` file is `docker/prometheus.yml`
>
>If you want to override these, you can pass in the following environment variables to the `docker-compose` command:
>
> `LOCAL_PROM_PORT`
>
> `LOCAL_M3COORD_PORT`
>
> `LOCAL_PROM_YML`
>
> (e.g. `$ LOCAL_PROM_PORT=XXXX LOCAL_M3COORD_PORT=XXXX LOCAL_PROM_YML=/path/to/yml docker-compose up`)

**Running m3coordinator locally (on mac only) and Prometheus in Docker container (for development):**

Build m3coordinator binary:

    $ make services

Run m3coordinator binary:

    $ ./bin/m3coordinator

Run Prometheus Docker image:

    $ docker run -p 9090:9090 -v $GOPATH/src/github.com/m3db/m3coordinator/docker/prometheus-mac.yml:/etc/prometheus/prometheus.yml quay.io/prometheus/prometheus

<hr>

This project is released under the [MIT License](LICENSE.md).

[doc-img]: https://godoc.org/github.com/m3db/m3coordinator?status.svg
[doc]: https://godoc.org/github.com/m3db/m3coordinator
[ci-img]: https://travis-ci.org/m3db/m3coordinator.svg?branch=master
[ci]: https://travis-ci.org/m3db/m3coordinator
[cov-img]: https://coveralls.io/repos/github/m3db/m3coordinator/badge.svg?branch=master&service=github
[cov]: https://coveralls.io/github/m3db/m3coordinator?branch=master
