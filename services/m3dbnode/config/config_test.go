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

	xtest "github.com/m3db/m3db/x/test"
	xconfig "github.com/m3db/m3x/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestConfiguration(t *testing.T) {
	in := `
logging:
    level: info
    file: /var/log/m3dbnode.log

metrics:
    m3:
        hostPort: 127.0.0.1:9052
        service: m3dbnode
        env: production
        includeHost: true
    samplingRate: 0.01
    runtime: simple

listenAddress: 0.0.0.0:9000
clusterListenAddress: 0.0.0.0:9001
httpNodeListenAddress: 0.0.0.0:9002
httpClusterListenAddress: 0.0.0.0:9003
debugListenAddress: 0.0.0.0:9004

hostID:
    resolver: hostname

client:
    writeConsistencyLevel: majority
    readConsistencyLevel: unstrict_majority
    clusterConnectConsistencyLevel: any
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
    retentionPeriod: 24h
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
    embeddedServer:
        listenPeerUrls:
            - http://0.0.0.0:2380
        listenClientUrls:
            - http://0.0.0.0:2379
        dir: /var/lib/etcd
        initialAdvertisePeerUrls:
            - http://1.1.1.1:2380
        advertiseClientUrls:
            - http://1.1.1.1:2379
        initialCluster: host1=http://1.1.1.1:2380,host2=http://1.1.1.2:2380,host3=http://1.1.1.3:2380
        name: host1
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
	err = xconfig.LoadFile(&cfg, fd.Name())
	require.NoError(t, err)

	// Verify a reverse output of the data matches what we expect
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)

	expected := `logging:
  file: /var/log/m3dbnode.log
  level: info
  fields: {}
metrics:
  scope: null
  m3:
    hostPort: 127.0.0.1:9052
    hostPorts: []
    service: m3dbnode
    env: production
    tags: {}
    queue: 0
    packetSize: 0
    includeHost: true
  samplingRate: 0.01
  extended: null
  sanitization: null
listenAddress: 0.0.0.0:9000
clusterListenAddress: 0.0.0.0:9001
httpNodeListenAddress: 0.0.0.0:9002
httpClusterListenAddress: 0.0.0.0:9003
debugListenAddress: 0.0.0.0:9004
hostID:
  resolver: hostname
  value: null
  envVarName: null
client:
  config:
    service: null
    static: null
    embeddedServer: null
    namespaceTimeout: 0s
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
  peers: null
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
  retentionPeriod: 24h0m0s
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
      keepAlive:
        enabled: false
        period: 0s
        jitter: 0s
        timeout: 0s
      tls: null
    m3sd:
      initTimeout: 0s
  static: null
  embeddedServer:
    dir: /var/lib/etcd
    initialAdvertisePeerUrls:
    - http://1.1.1.1:2380
    advertiseClientUrls:
    - http://1.1.1.1:2379
    listenPeerUrls:
    - http://0.0.0.0:2380
    listenClientUrls:
    - http://0.0.0.0:2379
    initialCluster: host1=http://1.1.1.1:2380,host2=http://1.1.1.2:2380,host3=http://1.1.1.3:2380
    name: host1
    clientsecurityjson:
      cafile: ""
      certfile: ""
      keyfile: ""
      certauth: false
      trustedcafile: ""
      autotls: false
    peersecurityjson:
      cafile: ""
      certfile: ""
      keyfile: ""
      certauth: false
      trustedcafile: ""
      autotls: false
  namespaceTimeout: 0s
hashing:
  seed: 42
writeNewSeriesAsync: true
`

	actual := string(data)
	if expected != actual {
		diff := xtest.Diff(expected, actual)
		require.FailNow(t, "reverse config did not match:\n"+diff)
	}
}

func TestInitialClusterToETCDEndpoints(t *testing.T) {
	endpoints, err := InitialClusterToETCDEndpoints("host1=http://1.1.1.1:2380")
	require.NoError(t, err)
	require.NotNil(t, endpoints)
	assert.Equal(t, 1, len(endpoints))
	assert.Equal(t, "http://1.1.1.1:2379", endpoints[0])

	endpoints, err = InitialClusterToETCDEndpoints("host1=http://1.1.1.1:2380,host2=http://1.1.1.2:2380,host3=http://1.1.1.3:2380")
	require.NoError(t, err)
	require.NotNil(t, endpoints)
	assert.Equal(t, 3, len(endpoints))
	assert.Equal(t, "http://1.1.1.1:2379", endpoints[0])
	assert.Equal(t, "http://1.1.1.2:2379", endpoints[1])
	assert.Equal(t, "http://1.1.1.3:2379", endpoints[2])

	_, err = InitialClusterToETCDEndpoints("")
	require.Error(t, err)

	_, err = InitialClusterToETCDEndpoints("host1")
	require.Error(t, err)

	_, err = InitialClusterToETCDEndpoints("http://1.1.1.1:2380")
	require.Error(t, err)
}

func TestIsETCDNode(t *testing.T) {
	res := IsETCDNode("host1=http://1.1.1.1:2380", "host1")
	assert.Equal(t, true, res)

	res = IsETCDNode("host1=http://1.1.1.1:2380,host2=http://1.1.1.2:2380,host3=http://1.1.1.3:2380", "host2")
	assert.Equal(t, true, res)

	res = IsETCDNode("host1=http://1.1.1.1:2380,host2=http://1.1.1.2:2380", "host4")
	assert.Equal(t, false, res)
}
