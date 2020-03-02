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
docker pull quay.io/m3db/m3dbnode:latest
docker run -p 7201:7201 -p 7203:7203 -p 9003:9003 --name m3db -v $(pwd)/m3db_data:/var/lib/m3db quay.io/m3db/m3dbnode:latest
```

**Note:** For the single node case, we use this [sample config file](https://github.com/m3db/m3/blob/master/src/dbnode/config/m3dbnode-local-etcd.yml). If you inspect the file, you'll see that all the configuration is namespaced by `coordinator` or `db`. That's because this setup runs `M3DB` and `M3Coordinator` as one application. While this is convenient for testing and development, you'll want to run clustered `M3DB` with a separate `M3Coordinator` in production. You can read more about that [here.](cluster_hard_way.md).

Next, create an initial namespace for your metrics in the database using the cURL below. Keep in mind that the provided `namespaceName` must match the namespace in the `local` section of the `M3Coordinator` YAML configuration, and if you choose to [add any additional namespaces](../operational_guide/namespace_configuration.md) you'll need to add them to the `local` section of `M3Coordinator`'s YAML config as well.

```json
curl -X POST http://localhost:7201/api/v1/database/create -d '{
  "type": "local",
  "namespaceName": "default",
  "retentionTime": "12h"
}'
```

**Note**: The `api/v1/database/create` endpoint is abstraction over two concepts in M3DB called [placements](../operational_guide/placement.md) and [namespaces](../operational_guide/namespace_configuration.md). If a placement doesn't exist, it will create one based on the `type` argument, otherwise if the placement already exists, it just creates the specified namespace. For now it's enough to just understand that it creates M3DB namespaces (tables), but if you're going to run a clustered M3 setup in production, make sure you familiarize yourself with the links above.

Placement initialization may take a minute or two and you can check on the status of this by running the following:

```
curl http://localhost:7201/api/v1/placement | jq .
```

Once all of the shards become `AVAILABLE`, you should see your node complete bootstrapping! Don't worry if you see warnings or errors related to a local cache file, such as `[W] could not load cache from file
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
curl -sS -X POST http://localhost:9003/writetagged -d '{
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
    "timestamp": '"$(date "+%s")"',
    "value": 42.123456789
  }
}
'
```

**Note:** In the above example we include the tag `__name__`. This is because `__name__` is a
reserved tag in Prometheus and will make querying the metric much easier. For example, if you have
[M3Query](query.md) setup as a Prometheus datasource in Grafana, you can then query for the metric
using the following PromQL query:

```
user_login{city="new_york",endpoint="/request"}
```

And reading the metrics you've written using the M3DB `/query` endpoint:
```json
curl -sS -X POST http://localhost:9003/query -d '{
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

Now that you've got the M3 stack up and running, take a look at the rest of our documentation to see how you can integrate with [Prometheus](../integrations/prometheus.md) and [Graphite](../integrations/graphite.md)
