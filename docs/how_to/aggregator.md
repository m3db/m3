# Setting up m3aggregator

## Introduction

m3aggregator is used to cluster stateful downsampling and rollup of metrics before they are store in M3DB. The m3coordinator also performs this role but is not cluster aware. This means metrics will not get aggregated properly if you send metrics in round round fashion to multiple m3coordinators for the same metrics ingestion source (e.g. Prometheus server).

Metrics sent to m3aggregator are correctly routed to the instance resposible for aggregating each metric and there is multiple m3aggregator replicas to ensure no single point of failure during aggregation.

## Configuration

Before setting up m3aggregator, make sure that you have at least [one M3DB node running](single_node.md) and a dedicated m3coordinator setup.

We highly recommend running with at least a replication factor 2 for a m3aggregator deployment.

### Topology

#### Initializing aggregator topology 
You can setup a m3aggregator topology by issuing a request to your coordinator (be sure to use your own hostnames, number of shards and replication factor):
```bash
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3coordinator01:7201/api/v1/services/m3aggregator/placement/init -d '{
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

#### Initializing m3msg topic for m3coordinator ingestion from m3aggregators
Now we must setup a topic for the m3aggregator to send computed metrics aggregations back to an m3coordinator instance for storage to M3DB:
```
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3coordinator01:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'
```

#### Initializing m3coordinator topology
Then the m3coordinators need to be configured to receive traffic for this topic (note ingest at port 7507 must match the configured port for your m3coordinator ingest server):
```
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3coordinator01:7201/api/v1/services/m3coordinator/placement/init -d '{
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
**Note:** When you add and remove m3coordinators they must be added to this placement.

#### Add m3coordinator consumer group to topic
Add the m3coordinator placement to receive traffic from the topic (make sure to set message TTL to match your desired maximum in memory message buffer):
```
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -X POST http://m3coordinator01:7201/api/v1/topic -d '{
  "consumerService": {
    "serviceId": {
      "name": "m3coordinator",
      "environment": "default_env",
      "zone": "embedded"
    },
    "consumptionType": "SHARED",
    "messageTtlNanos": "300000000000"
  }
}'
```

### Running
#### Dedicated Coordinator
Metrics will still arrive at the m3coordinator, they simply need to be forwarded to m3aggregators. The m3coordinator then also needs to receive metrics that have been aggregated from the m3aggregator and store them in M3DB, so running an ingestion server should be configured.

Here is the config you should add to your m3coordinator:
```
# This is for sending metrics to the remote m3aggregators
downsample:
  remoteAggregator:
    client:
      placementKV:
        namespace: /placement
      placementWatcher:
        key: m3aggregator
        initWatchTimeout: 10s
      hashType: murmur32
      shardCutoffLingerDuration: 1m
      flushSize: 1440
      maxTimerBatchSize: 1120
      queueSize: 10000
      queueDropType: oldest
      encoder:
        initBufferSize: 2048
        maxMessageSize: 10485760
        bytesPool:
          buckets:
            - capacity: 2048
              count: 4096
            - capacity: 4096
              count: 4096
          watermark:
            low: 0.7
            high: 1.0
      connection:
        writeTimeout: 250ms

# This is for configuring the ingestion server that will receive metrics from the m3aggregators on port 7507
ingest:
  ingester:
    workerPoolSize: 100
    opPool:
      size: 100
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

#### Aggregator
You can run m3aggregator by either building and running the binary yourself:

```
make m3aggregator
./bin/m3aggregator -f ./src/aggregator/config/m3aggregator.yml
```

Or you can run it with Docker using the Docker file located at `$GOPATH/src/github.com/m3db/m3/docker/m3aggregator/Dockerfile`.

## Usage

Send metrics as usual to your m3coordinators in round robin fashion (or any other load balancing strategy), the metrics will be forwarded to the m3aggregators, then once aggregated they will be sent to the m3coordinators to write to M3DB.
