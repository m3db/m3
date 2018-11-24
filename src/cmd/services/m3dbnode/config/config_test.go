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
	"io/ioutil"
	"os"
	"testing"

	"github.com/m3db/m3/src/dbnode/environment"
	xtest "github.com/m3db/m3/src/x/test"
	xconfig "github.com/m3db/m3x/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestConfiguration(t *testing.T) {
	in := `
db:
    logging:
        level: info
        file: /var/log/m3dbnode.log

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

    writeNewSeriesLimitPerSecond: 1048576
    writeNewSeriesBackoffDuration: 2ms

    bootstrap:
        bootstrappers:
            - filesystem
            - peers
            - noop-all
        fs:
            numProcessorsPerCPU: 0.125

    commitlog:
        flushMaxBytes: 524288
        flushEvery: 1s
        queue:
            calculationType: fixed
            size: 2097152
        blockSize: 10m

    fs:
        filePathPrefix: /var/lib/m3db
        writeBufferSize: 65536
        dataReadBufferSize: 65536
        infoReadBufferSize: 128
        seekReadBufferSize: 4096
        throughputLimitMbps: 100.0
        throughputCheckEvery: 128

    repair:
        enabled: false
        interval: 2h
        offset: 30m
        jitter: 1h
        throttle: 2m
        checkInterval: 1m

    pooling:
        blockAllocSize: 16
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
        closersPool:
            size: 104857
            lowWatermark: 0.01
            highWatermark: 0.02
        contextPool:
            size: 524288
            lowWatermark: 0.01
            highWatermark: 0.02
            maxFinalizerCapacity: 8
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
            capacity: 4096
            lowWatermark: 0.01
            highWatermark: 0.02
        hostBlockMetadataSlicePool:
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
            initialBatchSize: 128
            maxBatchSize: 100000
            pool:
              size: 8192
              lowWatermark: 0.01
              highWatermark: 0.02
        identifierPool:
            size: 9437184
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
                - hostID: host2
                  endpoint: http://1.1.1.2:2380
                - hostID: host3
                  endpoint: http://1.1.1.3:2380
    hashing:
      seed: 42
    writeNewSeriesAsync: true
`

	fd, err := ioutil.TempFile("", "config.yaml")
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fd.Name()))
	}()

	_, err = fd.Write([]byte(in))
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
  client:
    config:
      service: null
      static: null
      seedNodes: null
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
    backgroundHealthCheckFailLimit: 4
    backgroundHealthCheckFailThrottleFactor: 0.5
    hashing:
      seed: 42
  gcPercentage: 100
  writeNewSeriesLimitPerSecond: 1048576
  writeNewSeriesBackoffDuration: 2ms
  tick: null
  bootstrap:
    bootstrappers:
    - filesystem
    - peers
    - noop-all
    fs:
      numProcessorsPerCPU: 0.125
    commitlog: null
    cacheSeriesMetadata: null
  blockRetrieve: null
  cache:
    series: null
  fs:
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
  commitlog:
    flushMaxBytes: 524288
    flushEvery: 1s
    queue:
      calculationType: fixed
      size: 2097152
    queueChannel: null
    blockSize: 10m0s
  repair:
    enabled: false
    interval: 2h0m0s
    offset: 30m0s
    jitter: 1h0m0s
    throttle: 2m0s
    checkInterval: 1m0s
  pooling:
    blockAllocSize: 16
    type: simple
    bytesPool:
      buckets:
      - size: 6291456
        capacity: 16
        lowWatermark: 0.1
        highWatermark: 0.12
      - size: 3145728
        capacity: 32
        lowWatermark: 0.1
        highWatermark: 0.12
      - size: 3145728
        capacity: 64
        lowWatermark: 0.1
        highWatermark: 0.12
      - size: 3145728
        capacity: 128
        lowWatermark: 0.1
        highWatermark: 0.12
      - size: 3145728
        capacity: 256
        lowWatermark: 0.1
        highWatermark: 0.12
      - size: 524288
        capacity: 1440
        lowWatermark: 0.1
        highWatermark: 0.12
      - size: 524288
        capacity: 4096
        lowWatermark: 0.01
        highWatermark: 0.02
      - size: 32768
        capacity: 8192
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
      maxFinalizerCapacity: 8
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
      capacity: 32
      lowWatermark: 0.01
      highWatermark: 0.02
    fetchBlocksMetadataResultsPool:
      size: 32
      capacity: 4096
      lowWatermark: 0.01
      highWatermark: 0.02
    hostBlockMetadataSlicePool:
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
      initialBatchSize: 128
      maxBatchSize: 100000
      pool:
        size: 8192
        lowWatermark: 0.01
        highWatermark: 0.02
  config:
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
      m3sd:
        initTimeout: null
      watchWithRevision: 0
    static: null
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
      - hostID: host2
        endpoint: http://1.1.1.2:2380
      - hostID: host3
        endpoint: http://1.1.1.3:2380
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
