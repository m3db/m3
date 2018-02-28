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

// +build big

package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	hostID      = "m3dbtest01"
	serviceName = "m3dbnode_test"
	serviceEnv  = "test"
	serviceZone = "local"
	servicePort = 9000
	namespaceID = "metrics"
)

var testConfig = `
logging:
    level: info
    file: {{.LogFile}}

metrics:
    m3:
        hostPort: 127.0.0.1:9052
        service: m3dbnode
        env: production
        includeHost: true
    samplingRate: 0.01
    runtime: simple

listenAddress: 0.0.0.0:{{.ServicePort}}
clusterListenAddress: 0.0.0.0:9001
httpNodeListenAddress: 0.0.0.0:9002
httpClusterListenAddress: 0.0.0.0:9003
debugListenAddress: 0.0.0.0:9004

hostID:
    resolver: config
    value: {{.HostID}}

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

gcPercentage: 100

writeNewSeriesAsync: true
writeNewSeriesLimitPerSecond: 1048576
writeNewSeriesBackoffDuration: 2ms

bootstrap:
    bootstrappers:
        - filesystem
        - commitlog
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
    filePathPrefix: {{.DataDir}}
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
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    blockPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    encoderPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    closersPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    contextPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    segmentReaderPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    iteratorPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    fetchBlockMetadataResultsPool:
        size: 128
        capacity: 32
        lowWatermark: 0.01
        highWatermark: 0.02
    fetchBlocksMetadataResultsPool:
        size: 128
        capacity: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    hostBlockMetadataSlicePool:
        size: 128
        capacity: 3
        lowWatermark: 0.01
        highWatermark: 0.02
    blockMetadataPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    blockMetadataSlicePool:
        size: 128
        capacity: 32
        lowWatermark: 0.01
        highWatermark: 0.02
    blocksMetadataPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    blocksMetadataSlicePool:
        size: 128
        capacity: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    identifierPool:
        size: 128
        lowWatermark: 0.01
        highWatermark: 0.02
    bytesPool:
        buckets:
            - capacity: 32
              size: 128
            - capacity: 512
              size: 128
            - capacity: 4096
              size: 128

config:
    service:
        env: {{.ServiceEnv}}
        zone: {{.ServiceZone}}
        service: {{.ServiceName}}
        cacheDir: {{.ConfigServiceCacheDir}}
        etcdClusters:
            - zone: {{.ServiceZone}}
              endpoints: {{.EtcdEndpoints}}
`

type cleanup func()

func tempFile(t *testing.T, name string) (*os.File, cleanup) {
	fd, err := ioutil.TempFile("", name)
	require.NoError(t, err)

	fname := fd.Name()
	return fd, func() {
		assert.NoError(t, fd.Close())
		assert.NoError(t, os.Remove(fname))
	}
}

func tempFileTouch(t *testing.T, name string) (string, cleanup) {
	fd, err := ioutil.TempFile("", name)
	require.NoError(t, err)

	fname := fd.Name()
	require.NoError(t, fd.Close())
	return fname, func() {
		assert.NoError(t, os.Remove(fname))
	}
}

func tempDir(t *testing.T, name string) (string, cleanup) {
	dir, err := ioutil.TempDir("", name)
	require.NoError(t, err)
	return dir, func() {
		assert.NoError(t, os.RemoveAll(dir))
	}
}

// yamlArray returns a JSON array which is valid YAML, hehe..
func yamlArray(t *testing.T, values []string) string {
	buff := bytes.NewBuffer(nil)
	err := json.NewEncoder(buff).Encode(values)
	require.NoError(t, err)
	return strings.TrimSpace(buff.String())
}

func endpoint(ip string, port uint32) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func newNamespaceProtoValue(id string) (proto.Message, error) {
	md, err := namespace.NewMetadata(
		ident.StringID(id),
		namespace.NewOptions().
			SetNeedsBootstrap(true).
			SetNeedsFilesetCleanup(true).
			SetNeedsFlush(true).
			SetNeedsRepair(true).
			SetWritesToCommitLog(true).
			SetRetentionOptions(
				retention.NewOptions().
					SetBlockSize(1*time.Hour).
					SetRetentionPeriod(24*time.Hour)))
	if err != nil {
		return nil, err
	}
	nsMap, err := namespace.NewMap([]namespace.Metadata{md})
	if err != nil {
		return nil, err
	}
	return namespace.ToProto(nsMap), nil
}
