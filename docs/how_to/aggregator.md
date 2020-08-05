# Setting up M3 Aggregator

## Introduction

`m3aggregator` is used to cluster stateful downsampling and rollup of metrics before they are store in M3DB. The M3 Coordinator also performs this role but is not cluster aware. This means metrics will not get aggregated properly if you send metrics in round round fashion to multiple M3 Coordinators for the same metrics ingestion source (e.g. Prometheus server).

Metrics sent to `m3aggregator` are correctly routed to the instance resposible for aggregating each metric and there is multiple m3aggregator replicas to ensure no single point of failure during aggregation.

## Configuration

Before setting up m3aggregator, make sure that you have at least [one M3DB node running](single_node.md) and a dedicated m3coordinator setup.

We highly recommend running with at least a replication factor 2 for a `m3aggregator` deployment.

### Topology

#### Initializing aggregator topology 

You can setup a m3aggregator topology by issuing a request to your coordinator (be sure to use your own hostnames, number of shards and replication factor):
```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/services/m3aggregator/placement/init -d '{
    "num_shards": 64,
    "replication_factor": 2,
    "instances": [
        {
            "id": "m3aggregator01:6000",
            "isolation_group": "availability-zone-a",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3aggregator01:6000",
            "hostname": "m3aggregator01",
            "port": 6000
        },
        {
            "id": "m3aggregator02:6000",
            "isolation_group": "availability-zone-b",
            "zone": "embedded",
            "weight": 100,
            "endpoint": "m3aggregator02:6000",
            "hostname": "m3aggregator02",
            "port": 6000
        }
    ]
}'
```

#### Initializing m3msg topic for m3aggregator to receive from m3coordinators to aggregate metrics

Now we must setup a topic for the `m3aggregator` to receive unaggregated metrics from `m3coordinator` instances:

```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -H "Topic-Name: aggregator_ingest" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'
```

#### Initializing m3msg topic for m3coordinator to receive from m3aggregators to write to M3DB

Now we must setup a topic for the `m3aggregator` to send computed metrics aggregations back to an `m3coordinator` instance for storage to M3DB:
```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'
```

#### Add m3aggregagtor consumer group to ingest topic

Add the `m3aggregator` placement to receive traffic from the topic (make sure to set message TTL to match your desired maximum in memory retry message buffer):
```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -H "Topic-Name: aggregator_ingest" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic -d '{
  "consumerService": {
    "serviceId": {
      "name": "m3aggregator",
      "environment": "namespace/m3db-cluster-name",
      "zone": "embedded"
    },
    "consumptionType": "REPLICATED",
    "messageTtlNanos": "300000000000"
  }
}'
```

**Note:** 300000000000 nanoseconds is a TTL of 5 minutes for messages to rebuffer for retry.

#### Initializing m3msg topic for m3coordinator to receive from m3aggregator to write to M3DB

Now we must setup a topic for the `m3coordinator` to receive unaggregated metrics from `m3aggregator` instances to write to M3DB:
```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -H "Topic-Name: aggregated_metrics" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'
```

#### Initializing m3coordinator topology

Then `m3coordinator` instances need to be configured to receive traffic for this topic (note ingest at port 7507 must match the configured port for your `m3coordinator` ingest server, see config at bottom of this guide):
```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/services/m3coordinator/placement/init -d '{
    "instances": [
        {
            "id": "m3coordinator01",
            "zone": "embedded",
            "endpoint": "m3coordinator01:7507",
            "hostname": "m3coordinator01",
            "port": 7507
        }
    ]
}'
```

**Note:** When you add or remove `m3coordinator` instances they must be added to this placement.

#### Add m3coordinator consumer group to outbound topic

Add the `m3coordinator` placement to receive traffic from the topic (make sure to set message TTL to match your desired maximum in memory retry message buffer):
```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic -d '{
  "consumerService": {
    "serviceId": {
      "name": "m3coordinator",
      "environment": "namespace/m3db-cluster-name",
      "zone": "embedded"
    },
    "consumptionType": "SHARED",
    "messageTtlNanos": "300000000000"
  }
}'
```

**Note:** 300000000000 nanoseconds is a TTL of 5 minutes for messages to rebuffer for retry.

### Running

#### Dedicated Coordinator

Metrics will still arrive at the `m3coordinator`, they simply need to be forwarded to an `m3aggregator`. The `m3coordinator` then also needs to receive metrics that have been aggregated from the `m3aggregator` and store them in M3DB, so running an ingestion server should be configured.

Here is the config you should add to your `m3coordinator`:
```yaml
# This is for sending metrics to the remote m3aggregators
downsample:
  remoteAggregator:
    client:
      type: m3msg
      m3msg:
        producer:
          writer:
            topicName: aggregator_ingest
            topicServiceOverride:
              zone: embedded
              environment: namespace/m3db-cluster-name
            placement:
              isStaged: true
            placementServiceOverride:
              namespaces:
                placement: /placement
            connection:
              numConnections: 4
            messagePool:
              size: 16384
              watermark:
                low: 0.2
                high: 0.5

# This is for configuring the ingestion server that will receive metrics from the m3aggregators on port 7507
ingest:
  ingester:
    workerPoolSize: 10000
    opPool:
      size: 10000
    retry:
      maxRetries: 3
      jitter: true
    logSampleRate: 0.01
  m3msg:
    server:
      listenAddress: "0.0.0.0:7507"
      retry:
        maxBackoff: 10s
        jitter: true
```

#### M3 Aggregator

You can run `m3aggregator` by either building and running the binary yourself:

```bash
make m3aggregator
./bin/m3aggregator -f ./src/aggregator/config/m3aggregator.yml
```

Or you can run it with Docker using the Docker file located at `docker/m3aggregator/Dockerfile` or the publicly provided image `quay.io/m3db/m3aggregator:latest`.

## Usage

Send metrics as usual to your `m3coordinator` instances in round robin fashion (or any other load balancing strategy), the metrics will be forwarded to the `m3aggregator` instances, then once aggregated they will be returned to the `m3coordinator` instances to write to M3DB.
