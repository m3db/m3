## WARNING: This is Alpha software and not intended for use until a stable release.

# M3Coordinator [![GoDoc][doc-img]][doc]

M3Coordinator is a service which provides APIs for reading/writing to [M3DB](https://github.com/m3db/m3) at a global and placement specific level.
It also acts as a bridge between [Prometheus](https://github.com/prometheus/prometheus) and [M3DB](https://github.com/m3db/m3). Using this bridge, [M3DB](https://github.com/m3db/m3) acts as a long term storage for [Prometheus](https://github.com/prometheus/prometheus) using the [remote read/write endpoints](https://github.com/prometheus/prometheus/blob/master/prompb/remote.proto).
A detailed explanation of setting up long term storage for Prometheus can be found [here](https://schd.ws/hosted_files/cloudnativeeu2017/73/Integrating%20Long-Term%20Storage%20with%20Prometheus%20-%20CloudNativeCon%20Berlin%2C%20March%2030%2C%202017.pdf).

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

> Note: The default local ports for Prometheus and m3coordinator are `9090` and `7201`, respectively, and the default `prometheus.yml` file is `docker/prometheus.yml`
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

    $ ./bin/m3coordinator -f src/query/config/m3query-dev-etcd.yml

Run Prometheus Docker image:

    $ docker run -p 9090:9090 -v $GOPATH/src/github.com/m3db/m3/src/query/docker/prometheus-mac.yml:/etc/prometheus/prometheus.yml quay.io/prometheus/prometheus

### Running on GCP

Setup GCP for [single m3db node](https://github.com/m3db/m3/pull/452/files?short_path=20bfc3f#diff-20bfc3ff6a860483887b93bf9cf0d135)

> For a multi-node cluster, [see here](https://github.com/m3db/m3/src/query/tree/master/benchmark)

Setup GCP for m3coordinator:

    1. Make sure you select a base image with Docker pre-installed
    2. Follow steps 1-5 from the above section (clone `m3coordinator` instead of `m3db`)
        - Depending on the status of https://github.com/m3db/m3/pull/452, you may need to update the version of m3db (6874b8af8e9ec682551d49ad3e3250dfb4f4ae1f) and m3x (7ea8c2f35f9fa0f52bd189e44b11113d708acada) in `glide.yaml`
    3. The config file, which is located at `m3coordinator/benchmark/configs/benchmark.yml` will need the same config topology as the m3db config
    4. Run m3coordinator - you should see this message with the number of hosts you specified: `[I] successfully updated topology to 3 hosts` with no other warning or error messsages
        $ ./bin/m3coordinator --config.file benchmark/configs/benchmark.yml

Setup and run Prometheus:

    1. You can run Prometheus from the same box as m3coordinator
    2. Update the `prometheus.yml` in `m3coordinator/docker` so that the `remote_read` and `remote_write` endpoints are bound to the IP address of the host that m3coordinator is running on:
        - e.g.
            ```
            remote_read:
                - url: http://10.142.0.8:7201/api/v1/prom/remote/read

            remote_write:
                - url: http://10.142.0.8:7201/api/v1/prom/remote/write
            ```
    3. Run Prometheus
        $ sudo docker run -p 9090:9090 -v $GOPATH/src/github.com/m3db/m3/src/query/docker/prometheus.yml:/etc/prometheus/prometheus.yml quay.io/prometheus/prometheus

### Prom Validation Endpoint

Users can validate m3query's Prometheus results for a given query against input JSON through our validate endpoint. The input JSON consists of raw datapoints (i.e. Prom results without any functions applied) (`input`) and expected results (`results`). Along with this input and the given query, this endpoint will compare m3query's results that are calculated using the raw datapoints provided to the expected results. Below is an example:

Query: `go_gc_duration_seconds{quantile="1"} * 2`

Input JSON:

```
{
  "input": {
    "status": "success",
    "data": {
      "resultType": "matrix",
      "result": [
        {
          "metric": {
            "__name__": "go_gc_duration_seconds",
            "instance": "localhost:9090",
            "job": "prometheus",
            "quantile": "1"
          },
          "values": [
            [
              1543434961.200,
              "0.0032431"
            ],
            [
              1543434975.200,
              "0.0032431"
            ],
          ]
        }
      ]
    }
  },
  "results": {
    "status": "success",
    "data": {
      "resultType": "matrix",
      "result": [
        {
          "metric": {
            "__name__": "go_gc_duration_seconds",
            "instance": "localhost:9090",
            "job": "prometheus",
            "quantile": "1"
          },
          "values": [
            [
              1543434961.200,
              "0.0064862"
            ],
            [
              1543434975.200,
              "0.0064862"
            ],
          ]
        }
      ]
    }
  }
}
```

Full request: `curl -X POST 'localhost:7201/api/v1/debug/validate_query?start=1543431465&end=1543435045&step=14s&query=go_gc_duration_seconds%7Bquantile%3D%221%22%7D*2' -d @<input_json_file> --header "Content-Type: application/json"`

[doc-img]: https://godoc.org/github.com/m3db/m3/src/query?status.svg
[doc]: https://godoc.org/github.com/m3db/m3/src/query
