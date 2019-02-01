# M3DB Single Node Deployment

Deploying a single-node cluster is a great way to experiment with M3DB and get a feel for what it
has to offer. Our Docker image by default configures a single M3DB instance as one binary
containing:

- An M3DB storage instance (`m3dbnode`) for timeseries storage. This includes an embedded tag-based
  metrics index, as well as as an embedded etcd server for storing the above mentioned cluster
  topology and runtime configuration.
- A "coordinator" instance (`m3coordinator`) for writing and querying tagged metrics, as well as
  managing cluster topology and runtime configuration.

To begin, first start up a Docker container with port `7201` (used to manage the cluster topology), port `7203` which is where Prometheus scrapes metrics produced by `M3DB` and `M3Coordinator`, and port `9003` (used to read and write metrics) exposed. We recommend you create a persistent data
directory on your host for durability:

```
docker pull quay.io/m3/m3dbnode:latest
docker run -p 7201:7201 -p 7203:7203 -p 9003:9003 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db -v $GOPATH/src/github.com/m3db/m3/src/dbnode/config/m3dbnode-local-etcd.yml:/etc/m3dbnode/m3dbnode.yml quay.io/m3/m3dbnode:latest
```

**Note:** If you don't have `M3` setup in your `$GOPATH`, you can find the [m3dbnode-local-etcd.yml file here](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd.yml).

**Note:** This setup runs `M3DB` and `M3Coordinator` as one application and should only be used for testing/development purposes. If you want to run a clustered `M3DB` with a separate `M3Coordinator` process (which is our recommended production setup), please [see here](cluster_hard_way.md).

Next, create an initial namespace for your metrics in the database:

```json
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "48h"
}'
```

**Note:** If you want to create more than one namespace, you should follow the [instructions here](../operational_guide/namespace_configuration.md) and also add the namespace you created to the `local` section of the `m3dbnode-local-etcd.yml` file used in the `docker run` command above with the appropriate aggregation options specified - for more information on our aggregation functionality, check out our [M3Query documentation](query.md). For example:

<!-- TODO: link to aggregation documentation (outside of query.md) -->

```json
local:
  namespaces:
    - namespace: default
      type: unaggregated
      retention: 48h
    - namespace: <new_namespace>
      type: aggregated
      retention: <new_retention>
      resolution: <new_resolution>
```

Shortly after, you should see your node complete bootstrapping! Don't worry if you see warnings or
errors related to a local cache file, such as `[W] could not load cache from file
/var/lib/m3kv/m3db_embedded.json`. Those are expected for a local instance and in general any
warn-level errors (prefixed with `[W]`) should not block bootstrapping.

```
02:28:30.008072[I] updating database namespaces [{adds [default]} {updates []} {removals []}]
02:28:30.270681[I] node tchannelthrift: listening on 0.0.0.0:9000
02:28:30.271909[I] cluster tchannelthrift: listening on 0.0.0.0:9001
02:28:30.519468[I] node httpjson: listening on 0.0.0.0:9002
02:28:30.520061[I] cluster httpjson: listening on 0.0.0.0:9003
02:28:30.520652[I] bootstrap finished [{namespace metrics} {duration 55.4µs}]
02:28:30.520909[I] bootstrapped
```

The node also self-hosts its OpenAPI docs, outlining available endpoints. You can access this by
going to `localhost:7201/api/v1/openapi` in your browser.

![OpenAPI Doc](redoc_screenshot.png)

Now you can experiment with writing tagged metrics:
```json
curl -sSf -X POST http://localhost:9003/writetagged -d '{
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
    "timestamp": '"$(date "+%s")"',
    "value": 42.123456789
  }
}
'
```

And reading the metrics you've written:
```json
curl -sSf -X POST http://localhost:9003/query -d '{
  "namespace": "default",
  "query": {
    "regexp": {
      "field": "city",
      "regexp": ".*"
    }
  },
  "rangeStart": 0,
  "rangeEnd": '"$(date "+%s")"'
}' | jq .

{
  "results": [
    {
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
      "datapoints": [
        {
          "timestamp": 1527039389,
          "value": 42.123456789
        }
      ]
    }
  ],
  "exhaustive": true
}
```

## Integrations

[Prometheus as a long term storage remote read/write endpoint](../integrations/prometheus.md).
