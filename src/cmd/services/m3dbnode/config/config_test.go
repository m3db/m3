// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

const testBaseConfig = `
db:
  logging:
      level: info
      file: /var/log/m3dbnode.log

  cache:
      postingsList:
          size: 100
          cacheRegexp: false
          cacheTerms: false

  metrics:
      prometheus:
          handlerPath: /metrics
      sanitization: prometheus
      samplingRate: 1.0
      extended: detailed

  listenAddress: 0.0.0.0:9000
  clusterListenAddress: 0.0.0.0:9001
  httpNodeListenAddress: 0.0.0.0:9002
  httpClusterListenAddress: 0.0.0.0:9003
  debugListenAddress: 0.0.0.0:9004

  hostID:
      resolver: config
      value: host1

  client:
      writeConsistencyLevel: majority
      readConsistencyLevel: unstrict_majority
      connectConsistencyLevel: any
      writeTimeout: 10s
      fetchTimeout: 15s
      connectTimeout: 20s
      writeRetry:
          initialBackoff: 500ms
          backoffFactor: 3
          maxRetries: 2
          jitter: true
      fetchRetry:
          initialBackoff: 500ms
          backoffFactor: 2
          maxRetries: 3
          jitter: true
      backgroundHealthCheckFailLimit: 4
      backgroundHealthCheckFailThrottleFactor: 0.5
      hashing:
        seed: 42

  gcPercentage: 100

  bootstrap:
      filesystem:
          numProcessorsPerCPU: 0.42
      commitlog:
          returnUnfulfilledForCorruptCommitLogFiles: false

  commitlog:
      flushMaxBytes: 524288
      flushEvery: 1s
      queue:
          calculationType: fixed
          size: 2097152

  filesystem:
      filePathPrefix: /var/lib/m3db
      writeBufferSize: 65536
      dataReadBufferSize: 65536
      infoReadBufferSize: 128
      seekReadBufferSize: 4096
      throughputLimitMbps: 100.0
      throughputCheckEvery: 128
      force_index_summaries_mmap_memory: true
      force_bloom_filter_mmap_memory: true

  repair:
      enabled: false
      throttle: 2m
      checkInterval: 1m

  pooling:
      blockAllocSize: 16
      thriftBytesPoolAllocSize: 2048
      type: simple
      seriesPool:
          size: 5242880
          lowWatermark: 0.01
          highWatermark: 0.02
      blockPool:
          size: 4194304
          lowWatermark: 0.01
          highWatermark: 0.02
      encoderPool:
          size: 25165824
          lowWatermark: 0.01
          highWatermark: 0.02
      checkedBytesWrapperPool:
          size: 65536
          lowWatermark: 0.01
          highWatermark: 0.02
      closersPool:
          size: 104857
          lowWatermark: 0.01
          highWatermark: 0.02
      contextPool:
          size: 524288
          lowWatermark: 0.01
          highWatermark: 0.02
      segmentReaderPool:
          size: 16384
          lowWatermark: 0.01
          highWatermark: 0.02
      iteratorPool:
          size: 2048
          lowWatermark: 0.01
          highWatermark: 0.02
      fetchBlockMetadataResultsPool:
          size: 65536
          capacity: 32
          lowWatermark: 0.01
          highWatermark: 0.02
      fetchBlocksMetadataResultsPool:
          size: 32
          lowWatermark: 0.01
          highWatermark: 0.02
          capacity: 4096
      replicaMetadataSlicePool:
          size: 131072
          capacity: 3
          lowWatermark: 0.01
          highWatermark: 0.02
      blockMetadataPool:
          size: 65536
          lowWatermark: 0.01
          highWatermark: 0.02
      blockMetadataSlicePool:
          size: 65536
          capacity: 32
          lowWatermark: 0.01
          highWatermark: 0.02
      blocksMetadataPool:
          size: 65536
          lowWatermark: 0.01
          highWatermark: 0.02
      blocksMetadataSlicePool:
          size: 32
          capacity: 4096
          lowWatermark: 0.01
          highWatermark: 0.02
      tagsPool:
          size: 65536
          capacity: 8
          maxCapacity: 32
          lowWatermark: 0.01
          highWatermark: 0.02
      tagIteratorPool:
          size: 8192
          lowWatermark: 0.01
          highWatermark: 0.02
      indexResultsPool:
          size: 8192
          lowWatermark: 0.01
          highWatermark: 0.02
      tagEncoderPool:
          size: 8192
          lowWatermark: 0.01
          highWatermark: 0.02
      tagDecoderPool:
          size: 8192
          lowWatermark: 0.01
          highWatermark: 0.02
      writeBatchPool:
          size: 8192
          initialBatchSize: 128
          maxBatchSize: 100000
      postingsListPool:
          size: 8
          lowWatermark: 0
          highWatermark: 0
      identifierPool:
          size: 9437184
          lowWatermark: 0.01
          highWatermark: 0.02
      bufferBucketPool:
          size: 65536
          lowWatermark: 0.01
          highWatermark: 0.02
      bufferBucketVersionsPool:
          size: 65536
          lowWatermark: 0.01
          highWatermark: 0.02
      retrieveRequestPool:
          size: 65536
          lowWatermark: 0.01
          highWatermark: 0.02
      bytesPool:
          buckets:
              - capacity: 16
                size: 6291456
                lowWatermark: 0.10
                highWatermark: 0.12
              - capacity: 32
                size: 3145728
                lowWatermark: 0.10
                highWatermark: 0.12
              - capacity: 64
                size: 3145728
                lowWatermark: 0.10
                highWatermark: 0.12
              - capacity: 128
                size: 3145728
                lowWatermark: 0.10
                highWatermark: 0.12
              - capacity: 256
                size: 3145728
                lowWatermark: 0.10
                highWatermark: 0.12
              - capacity: 1440
                size: 524288
                lowWatermark: 0.10
                highWatermark: 0.12
              - capacity: 4096
                size: 524288
                lowWatermark: 0.01
                highWatermark: 0.02
              - capacity: 8192
                size: 32768
                lowWatermark: 0.01
                highWatermark: 0.02

  discovery:
    config:
        service:
            env: production
            zone: embedded
            service: m3db
            cacheDir: /var/lib/m3kv
            etcdClusters:
                - zone: embedded
                  endpoints:
                      - 1.1.1.1:2379
                      - 1.1.1.2:2379
                      - 1.1.1.3:2379

        seedNodes:
            listenPeerUrls:
                - http://0.0.0.0:2380
            listenClientUrls:
                - http://0.0.0.0:2379
            rootDir: /var/lib/etcd
            initialAdvertisePeerUrls:
                - http://1.1.1.1:2380
            advertiseClientUrls:
                - http://1.1.1.1:2379
            initialCluster:
                - hostID: host1
                  endpoint: http://1.1.1.1:2380
                  clusterState: existing
                - hostID: host2
                  endpoint: http://1.1.1.2:2380
                - hostID: host3
                  endpoint: http://1.1.1.3:2380
  hashing:
    seed: 42
  writeNewSeriesAsync: true
  writeNewSeriesBackoffDuration: 2ms
  tracing:
    backend: jaeger
`

func TestConfiguration(t *testing.T) {
	fd, err := ioutil.TempFile("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(testBaseConfig))
	require.NoError(t, err)

	// Verify is valid
	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	// Verify a reverse output of the data matches what we expect
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)

	expected := `db:
  index:
    maxQueryIDsConcurrency: 0
    regexpDFALimit: null
    regexpFSALimit: null
    forwardIndexProbability: 0
    forwardIndexThreshold: 0
  transforms:
    truncateBy: 0
    forceValue: null
  logging:
    file: /var/log/m3dbnode.log
    level: info
    fields: {}
  metrics:
    scope: null
    m3: null
    prometheus:
      handlerPath: /metrics
      listenAddress: ""
      timerType: ""
      defaultHistogramBuckets: []
      defaultSummaryObjectives: []
      onError: ""
    samplingRate: 1
    extended: 3
    sanitization: 2
  listenAddress: 0.0.0.0:9000
  clusterListenAddress: 0.0.0.0:9001
  httpNodeListenAddress: 0.0.0.0:9002
  httpClusterListenAddress: 0.0.0.0:9003
  debugListenAddress: 0.0.0.0:9004
  hostID:
    resolver: config
    value: host1
    envVarName: null
    file: null
    hostname: null
  client:
    config: null
    writeConsistencyLevel: 2
    readConsistencyLevel: 2
    connectConsistencyLevel: 0
    writeTimeout: 10s
    fetchTimeout: 15s
    connectTimeout: 20s
    writeRetry:
      initialBackoff: 500ms
      backoffFactor: 3
      maxBackoff: 0s
      maxRetries: 2
      forever: null
      jitter: true
    fetchRetry:
      initialBackoff: 500ms
      backoffFactor: 2
      maxBackoff: 0s
      maxRetries: 3
      forever: null
      jitter: true
    logErrorSampleRate: 0
    backgroundHealthCheckFailLimit: 4
    backgroundHealthCheckFailThrottleFactor: 0.5
    hashing:
      seed: 42
    proto: null
    asyncWriteWorkerPoolSize: null
    asyncWriteMaxConcurrency: null
    useV2BatchAPIs: null
    writeTimestampOffset: null
    fetchSeriesBlocksBatchConcurrency: null
    fetchSeriesBlocksBatchSize: null
    writeShardsInitializing: null
    shardsLeavingCountTowardsConsistency: null
  gcPercentage: 100
  tick: null
  bootstrap:
    mode: null
    filesystem:
      numProcessorsPerCPU: 0.42
      migration: null
    commitlog:
      returnUnfulfilledForCorruptCommitLogFiles: false
    peers: null
    cacheSeriesMetadata: null
    indexSegmentConcurrency: null
    verify: null
  blockRetrieve: null
  cache:
    series: null
    postingsList:
      size: 100
      cacheRegexp: false
      cacheTerms: false
    regexp: null
  filesystem:
    filePathPrefix: /var/lib/m3db
    writeBufferSize: 65536
    dataReadBufferSize: 65536
    infoReadBufferSize: 128
    seekReadBufferSize: 4096
    throughputLimitMbps: 100
    throughputCheckEvery: 128
    newFileMode: null
    newDirectoryMode: null
    mmap: null
    force_index_summaries_mmap_memory: true
    force_bloom_filter_mmap_memory: true
    bloomFilterFalsePositivePercent: null
  commitlog:
    flushMaxBytes: 524288
    flushEvery: 1s
    queue:
      calculationType: fixed
      size: 2097152
    queueChannel: null
  repair:
    enabled: false
    throttle: 2m0s
    checkInterval: 1m0s
    debugShadowComparisonsEnabled: false
    debugShadowComparisonsPercentage: 0
  replication: null
  pooling:
    blockAllocSize: 16
    thriftBytesPoolAllocSize: 2048
    type: simple
    bytesPool:
      buckets:
      - size: 6291456
        lowWatermark: 0.1
        highWatermark: 0.12
        capacity: 16
      - size: 3145728
        lowWatermark: 0.1
        highWatermark: 0.12
        capacity: 32
      - size: 3145728
        lowWatermark: 0.1
        highWatermark: 0.12
        capacity: 64
      - size: 3145728
        lowWatermark: 0.1
        highWatermark: 0.12
        capacity: 128
      - size: 3145728
        lowWatermark: 0.1
        highWatermark: 0.12
        capacity: 256
      - size: 524288
        lowWatermark: 0.1
        highWatermark: 0.12
        capacity: 1440
      - size: 524288
        lowWatermark: 0.01
        highWatermark: 0.02
        capacity: 4096
      - size: 32768
        lowWatermark: 0.01
        highWatermark: 0.02
        capacity: 8192
    checkedBytesWrapperPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
    closersPool:
      size: 104857
      lowWatermark: 0.01
      highWatermark: 0.02
    contextPool:
      size: 524288
      lowWatermark: 0.01
      highWatermark: 0.02
    seriesPool:
      size: 5242880
      lowWatermark: 0.01
      highWatermark: 0.02
    blockPool:
      size: 4194304
      lowWatermark: 0.01
      highWatermark: 0.02
    encoderPool:
      size: 25165824
      lowWatermark: 0.01
      highWatermark: 0.02
    iteratorPool:
      size: 2048
      lowWatermark: 0.01
      highWatermark: 0.02
    segmentReaderPool:
      size: 16384
      lowWatermark: 0.01
      highWatermark: 0.02
    identifierPool:
      size: 9437184
      lowWatermark: 0.01
      highWatermark: 0.02
    fetchBlockMetadataResultsPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
      capacity: 32
    fetchBlocksMetadataResultsPool:
      size: 32
      lowWatermark: 0.01
      highWatermark: 0.02
      capacity: 4096
    replicaMetadataSlicePool:
      size: 131072
      lowWatermark: 0.01
      highWatermark: 0.02
      capacity: 3
    blockMetadataPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
    blockMetadataSlicePool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
      capacity: 32
    blocksMetadataPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
    blocksMetadataSlicePool:
      size: 32
      lowWatermark: 0.01
      highWatermark: 0.02
      capacity: 4096
    tagsPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
      capacity: 8
      maxCapacity: 32
    tagIteratorPool:
      size: 8192
      lowWatermark: 0.01
      highWatermark: 0.02
    indexResultsPool:
      size: 8192
      lowWatermark: 0.01
      highWatermark: 0.02
    tagEncoderPool:
      size: 8192
      lowWatermark: 0.01
      highWatermark: 0.02
    tagDecoderPool:
      size: 8192
      lowWatermark: 0.01
      highWatermark: 0.02
    writeBatchPool:
      size: 8192
      initialBatchSize: 128
      maxBatchSize: 100000
    bufferBucketPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
    bufferBucketVersionsPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
    retrieveRequestPool:
      size: 65536
      lowWatermark: 0.01
      highWatermark: 0.02
    postingsListPool:
      size: 8
      lowWatermark: 0
      highWatermark: 0
  discovery:
    type: null
    m3dbCluster: null
    m3AggregatorCluster: null
    config:
      services:
      - async: false
        clientOverrides:
          hostQueueFlushInterval: null
          targetHostQueueFlushSize: null
        service:
          zone: embedded
          env: production
          service: m3db
          cacheDir: /var/lib/m3kv
          etcdClusters:
          - zone: embedded
            endpoints:
            - 1.1.1.1:2379
            - 1.1.1.2:2379
            - 1.1.1.3:2379
            keepAlive: null
            tls: null
            autoSyncInterval: 0s
          m3sd:
            initTimeout: null
          watchWithRevision: 0
          newDirectoryMode: null
          retry:
            initialBackoff: 0s
            backoffFactor: 0
            maxBackoff: 0s
            maxRetries: 0
            forever: null
            jitter: null
          requestTimeout: 0s
          watchChanInitTimeout: 0s
          watchChanCheckInterval: 0s
          watchChanResetInterval: 0s
          enableFastGets: false
      statics: []
      seedNodes:
        rootDir: /var/lib/etcd
        initialAdvertisePeerUrls:
        - http://1.1.1.1:2380
        advertiseClientUrls:
        - http://1.1.1.1:2379
        listenPeerUrls:
        - http://0.0.0.0:2380
        listenClientUrls:
        - http://0.0.0.0:2379
        initialCluster:
        - hostID: host1
          endpoint: http://1.1.1.1:2380
          clusterState: existing
        - hostID: host2
          endpoint: http://1.1.1.2:2380
          clusterState: ""
        - hostID: host3
          endpoint: http://1.1.1.3:2380
          clusterState: ""
        clientTransportSecurity:
          caFile: ""
          certFile: ""
          keyFile: ""
          trustedCaFile: ""
          clientCertAuth: false
          autoTls: false
        peerTransportSecurity:
          caFile: ""
          certFile: ""
          keyFile: ""
          trustedCaFile: ""
          clientCertAuth: false
          autoTls: false
  hashing:
    seed: 42
  writeNewSeriesAsync: true
  writeNewSeriesBackoffDuration: 2ms
  proto: null
  tracing:
    serviceName: ""
    backend: jaeger
    jaeger:
      serviceName: ""
      disabled: false
      rpc_metrics: false
      tags: []
      sampler: null
      reporter: null
      headers: null
      baggage_restrictions: null
      throttler: null
    lightstep:
      access_token: ""
      collector:
        scheme: ""
        host: ""
        port: 0
        plaintext: false
        custom_ca_cert_file: ""
      tags: {}
      lightstep_api:
        scheme: ""
        host: ""
        port: 0
        plaintext: false
        custom_ca_cert_file: ""
      max_buffered_spans: 0
      max_log_key_len: 0
      max_log_value_len: 0
      max_logs_per_span: 0
      grpc_max_call_send_msg_size_bytes: 0
      reporting_period: 0s
      min_reporting_period: 0s
      report_timeout: 0s
      drop_span_logs: false
      verbose: false
      use_http: false
      usegrpc: false
      reconnect_period: 0s
      meta_event_reporting_enabled: false
  limits:
    maxRecentlyQueriedSeriesDiskBytesRead: null
    maxRecentlyQueriedSeriesBlocks: null
    maxOutstandingWriteRequests: 0
    maxOutstandingReadRequests: 0
    maxOutstandingRepairedBytes: 0
    maxEncodersPerBlock: 0
    writeNewSeriesPerSecond: 0
  wide: null
  tchannel: null
  debug:
    mutexProfileFraction: 0
    blockProfileRate: 0
  forceColdWritesEnabled: null
coordinator: null
`

	actual := string(data)
	if expected != actual {
		diff := xtest.Diff(expected, actual)
		require.FailNow(t, "reverse config did not match:\n"+diff)
	}
}

func TestInitialClusterEndpoints(t *testing.T) {
	seedNodes := []environment.SeedNode{
		environment.SeedNode{
			HostID:   "host1",
			Endpoint: "http://1.1.1.1:2380",
		},
	}
	endpoints, err := InitialClusterEndpoints(seedNodes)
	require.NoError(t, err)
	require.NotNil(t, endpoints)
	require.Equal(t, 1, len(endpoints))
	assert.Equal(t, "http://1.1.1.1:2379", endpoints[0])

	seedNodes = []environment.SeedNode{
		environment.SeedNode{
			HostID:   "host1",
			Endpoint: "http://1.1.1.1:2380",
		},
		environment.SeedNode{
			HostID:   "host2",
			Endpoint: "http://1.1.1.2:2380",
		},
		environment.SeedNode{
			HostID:   "host3",
			Endpoint: "http://1.1.1.3:2380",
		},
	}
	endpoints, err = InitialClusterEndpoints(seedNodes)
	require.NoError(t, err)
	require.NotNil(t, endpoints)
	require.Equal(t, 3, len(endpoints))
	assert.Equal(t, "http://1.1.1.1:2379", endpoints[0])
	assert.Equal(t, "http://1.1.1.2:2379", endpoints[1])
	assert.Equal(t, "http://1.1.1.3:2379", endpoints[2])

	seedNodes = []environment.SeedNode{}
	endpoints, err = InitialClusterEndpoints(seedNodes)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(endpoints))

	seedNodes = []environment.SeedNode{
		environment.SeedNode{
			HostID:   "host1",
			Endpoint: "",
		},
	}
	_, err = InitialClusterEndpoints(seedNodes)
	require.Error(t, err)
}

func TestIsSeedNode(t *testing.T) {
	seedNodes := []environment.SeedNode{
		environment.SeedNode{
			HostID:   "host1",
			Endpoint: "http://1.1.1.1:2380",
		},
	}
	res := IsSeedNode(seedNodes, "host1")
	assert.Equal(t, true, res)

	seedNodes = []environment.SeedNode{
		environment.SeedNode{
			HostID:   "host1",
			Endpoint: "http://1.1.1.1:2380",
		},
		environment.SeedNode{
			HostID:   "host2",
			Endpoint: "http://1.1.1.2:2380",
		},
		environment.SeedNode{
			HostID:   "host3",
			Endpoint: "http://1.1.1.3:2380",
		},
	}
	res = IsSeedNode(seedNodes, "host2")
	assert.Equal(t, true, res)

	seedNodes = []environment.SeedNode{
		environment.SeedNode{
			HostID:   "host1",
			Endpoint: "http://1.1.1.1:2380",
		},
		environment.SeedNode{
			HostID:   "host2",
			Endpoint: "http://1.1.1.2:2380",
		},
	}
	res = IsSeedNode(seedNodes, "host4")
	assert.Equal(t, false, res)
}

func TestGetHostAndEndpointFromID(t *testing.T) {
	test2Seeds := []environment.SeedNode{
		environment.SeedNode{
			HostID:       "host1",
			Endpoint:     "http://1.1.1.1:2380",
			ClusterState: "existing",
		},
		environment.SeedNode{
			HostID:   "host2",
			Endpoint: "http://1.1.1.2:2380",
		},
	}

	tests := []struct {
		initialCluster []environment.SeedNode
		hostID         string
		expSeedNode    environment.SeedNode
		expEndpoint    string
		expErr         bool
	}{
		{
			initialCluster: test2Seeds,
			hostID:         "host1",
			expSeedNode:    test2Seeds[0],
			expEndpoint:    "http://1.1.1.1",
		},
		{
			initialCluster: test2Seeds,
			hostID:         "host3",
			expErr:         true,
		},
		{
			initialCluster: test2Seeds[:0],
			hostID:         "host1",
			expErr:         true,
		},
	}

	for _, test := range tests {
		node, ep, err := getHostAndEndpointFromID(test.initialCluster, test.hostID)
		if test.expErr {
			assert.Error(t, err)
			continue
		}

		assert.Equal(t, test.expSeedNode, node)
		assert.Equal(t, test.expEndpoint, ep)
	}
}

func TestNewEtcdEmbedConfig(t *testing.T) {
	fd, err := ioutil.TempFile("", "config2.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(testBaseConfig))
	require.NoError(t, err)

	// Verify is valid
	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	embedCfg, err := NewEtcdEmbedConfig(*cfg.DB)
	require.NoError(t, err)

	assert.Equal(t, "existing", embedCfg.ClusterState)
}

func TestNewJaegerTracer(t *testing.T) {
	fd, err := ioutil.TempFile("", "config_jaeger.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(testBaseConfig))
	require.NoError(t, err)

	// Verify is valid
	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	instrumentOpts := instrument.NewOptions()
	tracer, closer, err := cfg.DB.Tracing.NewTracer("m3dbnode",
		instrumentOpts.MetricsScope(), instrumentOpts.Logger())
	require.NoError(t, err)
	defer closer.Close()

	// Verify tracer gets created
	require.NotNil(t, tracer)
}

func TestProtoConfig(t *testing.T) {
	testProtoConf := `
db:
  metrics:
      samplingRate: 1.0

  listenAddress: 0.0.0.0:9000
  clusterListenAddress: 0.0.0.0:9001
  httpNodeListenAddress: 0.0.0.0:9002
  httpClusterListenAddress: 0.0.0.0:9003

  commitlog:
      flushMaxBytes: 524288
      flushEvery: 1s
      queue:
          size: 2097152

  proto:
      enabled: false
      schema_registry:
         "ns1:2d":
            schemaFilePath: "file/path/to/ns1/schema"
            messageName: "ns1_msg_name"
         ns2:
            schemaFilePath: "file/path/to/ns2/schema"
            messageName: "ns2_msg_name"
`
	fd, err := ioutil.TempFile("", "config_proto.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(testProtoConf))
	require.NoError(t, err)

	// Verify is valid
	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)

	require.NotNil(t, cfg.DB.Proto)
	require.False(t, cfg.DB.Proto.Enabled)

	require.Len(t, cfg.DB.Proto.SchemaRegistry, 2)
	require.EqualValues(t, map[string]NamespaceProtoSchema{
		"ns1:2d": {
			SchemaFilePath: "file/path/to/ns1/schema",
			MessageName:    "ns1_msg_name",
		},
		"ns2": {
			SchemaFilePath: "file/path/to/ns2/schema",
			MessageName:    "ns2_msg_name",
		}}, cfg.DB.Proto.SchemaRegistry)
}

func TestBootstrapCommitLogConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	notDefault := !commitlog.DefaultReturnUnfulfilledForCorruptCommitLogFiles
	notDefaultStr := fmt.Sprintf("%v", notDefault)

	testConf := `
db:
  metrics:
      samplingRate: 1.0

  listenAddress: 0.0.0.0:9000
  clusterListenAddress: 0.0.0.0:9001
  httpNodeListenAddress: 0.0.0.0:9002
  httpClusterListenAddress: 0.0.0.0:9003

  bootstrap:
      commitlog:
          returnUnfulfilledForCorruptCommitLogFiles: ` + notDefaultStr + `

  commitlog:
      flushMaxBytes: 524288
      flushEvery: 1s
      queue:
          size: 2097152
`
	fd, err := ioutil.TempFile("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(testConf))
	require.NoError(t, err)

	// Verify is valid
	var cfg Configuration
	err = xconfig.LoadFile(&cfg, fd.Name(), xconfig.Options{})
	require.NoError(t, err)
	require.NotNil(t, cfg.DB)

	mapProvider := topology.NewMockMapProvider(ctrl)
	origin := topology.NewMockHost(ctrl)
	adminClient := client.NewMockAdminClient(ctrl)

	_, err = cfg.DB.Bootstrap.New(
		result.NewOptions(),
		storage.DefaultTestOptions(),
		mapProvider,
		origin,
		adminClient,
	)
	require.NoError(t, err)
}
