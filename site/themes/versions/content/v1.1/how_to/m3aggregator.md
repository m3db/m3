---
title: Aggregate Metrics with M3 Aggregator
weight: 5
---

{{< fileinclude "docs/includes/m3aggregator_intro.md" >}}

## Configuration

Before setting up m3aggregator, make sure that you have at least [one M3DB node running](/docs/quickstart) and a dedicated m3coordinator setup.

We highly recommend running with at least a replication factor 2 for a `m3aggregator` deployment. If you run with replication factor 1 then when you restart an aggregator it will temporarily interrupt good the stream of aggregated metrics and there will be some data loss.

### Topology

#### Initializing aggregator topology 

You can setup a m3aggregator topology by issuing a request to your coordinator (be sure to use your own hostnames, number of shards and replication factor):
```shell
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

```shell
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -H "Topic-Name: aggregator_ingest" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'
```

#### Add m3aggregator consumer group to ingest topic

Add the `m3aggregator` placement to receive traffic from the topic (make sure to set message TTL to match your desired maximum in memory retry message buffer):
```shell
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

Now we must setup a topic for the `m3coordinator` to receive aggregated metrics from `m3aggregator` instances to write to M3DB:
```shell
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -H "Topic-Name: aggregated_metrics" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic/init -d '{
    "numberOfShards": 64
}'
```

#### Initializing m3coordinator topology

Then `m3coordinator` instances need to be configured to receive traffic for this topic (note ingest at port 7507 must match the configured port for your `m3coordinator` ingest server, see config at bottom of this guide):
```shell
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
```shell
curl -vvvsSf -H "Cluster-Environment-Name: namespace/m3db-cluster-name" -H "Topic-Name: aggregated_metrics" -X POST http://m3dbnode-with-embedded-coordinator:7201/api/v1/topic -d '{
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

```shell
make m3aggregator
./bin/m3aggregator -f ./src/aggregator/config/m3aggregator.yml
```

Or you can run it with Docker using the Docker file located at `docker/m3aggregator/Dockerfile` or the publicly provided image `quay.io/m3db/m3aggregator:latest`.

You can use a config like so, making note of the topics used such as `aggregator_ingest` and `aggregated_metrics` and the corresponding environment `namespace/m3db-cluster-name`:

```yaml
logging:
  level: info

metrics:
  scope:
    prefix: m3aggregator
  prometheus:
    onError: none
    handlerPath: /metrics
    listenAddress: 0.0.0.0:6002
    timerType: histogram
  sanitization: prometheus
  samplingRate: 1.0
  extended: none

m3msg:
  server:
    listenAddress: 0.0.0.0:6000
    retry:
      maxBackoff: 10s
      jitter: true
  consumer:
    messagePool:
      size: 16384
      watermark:
        low: 0.2
        high: 0.5

http:
  listenAddress: 0.0.0.0:6001
  readTimeout: 60s
  writeTimeout: 60s

kvClient:
  etcd:
    env: namespace/m3db-cluster-name
    zone: embedded
    service: m3aggregator
    cacheDir: /var/lib/m3kv
    etcdClusters:
      - zone: embedded
        endpoints:
          - dbnode01:2379

runtimeOptions:
  kvConfig:
    environment: namespace/m3db-cluster-name
    zone: embedded
  writeValuesPerMetricLimitPerSecondKey: write-values-per-metric-limit-per-second
  writeValuesPerMetricLimitPerSecond: 0
  writeNewMetricLimitClusterPerSecondKey: write-new-metric-limit-cluster-per-second
  writeNewMetricLimitClusterPerSecond: 0
  writeNewMetricNoLimitWarmupDuration: 0

aggregator:
  hostID:
    resolver: environment
    envVarName: M3AGGREGATOR_HOST_ID
  instanceID:
    type: host_id
  verboseErrors: true
  metricPrefix: ""
  counterPrefix: ""
  timerPrefix: ""
  gaugePrefix: ""
  aggregationTypes:
    counterTransformFnType: empty
    timerTransformFnType: suffix
    gaugeTransformFnType: empty
    aggregationTypesPool:
      size: 1024
    quantilesPool:
      buckets:
        - count: 256
          capacity: 4
        - count: 128
          capacity: 8
  stream:
    eps: 0.001
    capacity: 32
    streamPool:
      size: 4096
    samplePool:
      size: 4096
    floatsPool:
      buckets:
        - count: 4096
          capacity: 16
        - count: 2048
          capacity: 32
        - count: 1024
          capacity: 64
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
          messagePool:
            size: 16384
            watermark:
              low: 0.2
              high: 0.5
  placementManager:
    kvConfig:
      namespace: /placement
      environment: namespace/m3db-cluster-name
      zone: embedded
    placementWatcher:
      key: m3aggregator
      initWatchTimeout: 10s
  hashType: murmur32
  bufferDurationBeforeShardCutover: 10m
  bufferDurationAfterShardCutoff: 10m
  bufferDurationForFutureTimedMetric: 10m # Allow test to write into future.
  resignTimeout: 1m
  flushTimesManager:
    kvConfig:
      environment: namespace/m3db-cluster-name
      zone: embedded
    flushTimesKeyFmt: shardset/%d/flush
    flushTimesPersistRetrier:
      initialBackoff: 100ms
      backoffFactor: 2.0
      maxBackoff: 2s
      maxRetries: 3
  electionManager:
    election:
      leaderTimeout: 10s
      resignTimeout: 10s
      ttlSeconds: 10
    serviceID:
      name: m3aggregator
      environment: namespace/m3db-cluster-name
      zone: embedded
    electionKeyFmt: shardset/%d/lock
    campaignRetrier:
      initialBackoff: 100ms
      backoffFactor: 2.0
      maxBackoff: 2s
      forever: true
      jitter: true
    changeRetrier:
      initialBackoff: 100ms
      backoffFactor: 2.0
      maxBackoff: 5s
      forever: true
      jitter: true
    resignRetrier:
      initialBackoff: 100ms
      backoffFactor: 2.0
      maxBackoff: 5s
      forever: true
      jitter: true
    campaignStateCheckInterval: 1s
    shardCutoffCheckOffset: 30s
  flushManager:
    checkEvery: 1s
    jitterEnabled: true
    maxJitters:
      - flushInterval: 5s
        maxJitterPercent: 1.0
      - flushInterval: 10s
        maxJitterPercent: 0.5
      - flushInterval: 1m
        maxJitterPercent: 0.5
      - flushInterval: 10m
        maxJitterPercent: 0.5
      - flushInterval: 1h
        maxJitterPercent: 0.25
    numWorkersPerCPU: 0.5
    flushTimesPersistEvery: 10s
    maxBufferSize: 5m
    forcedFlushWindowSize: 10s
  flush:
    handlers:
      - dynamicBackend:
          name: m3msg
          hashType: murmur32
          producer:
            writer:
              topicName: aggregated_metrics
              topicServiceOverride:
                zone: embedded
                environment: namespace/m3db-cluster-name
              messagePool:
                size: 16384
                watermark:
                  low: 0.2
                  high: 0.5
  passthrough:
    enabled: true
  forwarding:
    maxConstDelay: 5m # Need to add some buffer window, since timed metrics by default are delayed by 1min.
  entryTTL: 1h
  entryCheckInterval: 10m
  maxTimerBatchSizePerWrite: 140
  defaultStoragePolicies: []
  maxNumCachedSourceSets: 2
  discardNaNAggregatedValues: true
  entryPool:
    size: 4096
  counterElemPool:
    size: 4096
  timerElemPool:
    size: 4096
  gaugeElemPool:
    size: 4096
```

## Usage

Send metrics as usual to your `m3coordinator` instances in round robin fashion (or any other load balancing strategy), the metrics will be forwarded to the `m3aggregator` instances, then once aggregated they will be returned to the `m3coordinator` instances to write to M3DB.
