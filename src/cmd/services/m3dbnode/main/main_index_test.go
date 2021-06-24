// +build big
//
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

package main_test

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/m3db/m3/src/cluster/integration/etcd"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/dbnode/server"
	dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	m3ninxidx "github.com/m3db/m3/src/m3ninx/idx"
	xconfig "github.com/m3db/m3/src/x/config"
	xcontext "github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestIndexEnabledServer tests booting a server using file based configuration.
func TestIndexEnabledServer(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Embedded kv
	embeddedKV, err := etcd.New(etcd.NewOptions())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, embeddedKV.Close())
	}()
	require.NoError(t, embeddedKV.Start())

	// Create config file
	tmpl, err := template.New("config").Parse(indexTestConfig)
	require.NoError(t, err)

	configFd, cleanup := tempFile(t, "config.yaml")
	defer cleanup()

	logFile, cleanupLogFile := tempFileTouch(t, "m3dbnode.log")
	defer cleanupLogFile()

	configServiceCacheDir, cleanupConfigServiceCacheDir := tempDir(t, "kv")
	defer cleanupConfigServiceCacheDir()

	dataDir, cleanupDataDir := tempDir(t, "data")
	defer cleanupDataDir()

	servicePort := nextServicePort()
	err = tmpl.Execute(configFd, struct {
		HostID                string
		LogFile               string
		DataDir               string
		ServicePort           string
		ServiceName           string
		ServiceEnv            string
		ServiceZone           string
		ConfigServiceCacheDir string
		EtcdEndpoints         string
	}{
		HostID:                hostID,
		LogFile:               logFile,
		DataDir:               dataDir,
		ServicePort:           strconv.Itoa(int(servicePort)),
		ServiceName:           serviceName,
		ServiceEnv:            serviceEnv,
		ServiceZone:           serviceZone,
		ConfigServiceCacheDir: configServiceCacheDir,
		EtcdEndpoints:         yamlArray(t, embeddedKV.Endpoints()),
	})
	require.NoError(t, err)

	// Setup the placement
	var cfg config.Configuration
	err = xconfig.LoadFile(&cfg, configFd.Name(), xconfig.Options{})
	require.NoError(t, err)

	discoveryCfg := cfg.DB.DiscoveryOrDefault()
	envCfg, err := discoveryCfg.EnvironmentConfig(hostID)
	require.NoError(t, err)

	syncCluster, err := envCfg.Services.SyncCluster()
	require.NoError(t, err)
	configSvcClient, err := syncCluster.Service.NewClient(instrument.NewOptions().
		SetLogger(zap.NewNop()))
	require.NoError(t, err)

	svcs, err := configSvcClient.Services(services.NewOverrideOptions())
	require.NoError(t, err)

	serviceID := services.NewServiceID().
		SetName(serviceName).
		SetEnvironment(serviceEnv).
		SetZone(serviceZone)

	metadata := services.NewMetadata().
		SetPort(servicePort).
		SetLivenessInterval(time.Minute).
		SetHeartbeatInterval(10 * time.Second)

	err = svcs.SetMetadata(serviceID, metadata)
	require.NoError(t, err)

	placementOpts := placement.NewOptions().
		SetValidZone(serviceZone)
	placementSvc, err := svcs.PlacementService(serviceID, placementOpts)
	require.NoError(t, err)

	var (
		instance = placement.NewInstance().
				SetID(hostID).
				SetEndpoint(endpoint("127.0.0.1", servicePort)).
				SetPort(servicePort).
				SetIsolationGroup("local").
				SetWeight(1).
				SetZone(serviceZone)
		instances = []placement.Instance{instance}
		// Keep number of shards low to avoid having to tune F.D limits.
		shards   = 4
		replicas = 1
	)

	_, err = placementSvc.BuildInitialPlacement(instances, shards, replicas)
	require.NoError(t, err)

	// Setup the namespace
	ns, err := newNamespaceWithIndexProtoValue(namespaceID, true)
	require.NoError(t, err)

	kvStore, err := configSvcClient.KV()
	require.NoError(t, err)

	_, err = kvStore.Set(kvconfig.NamespacesKey, ns)
	require.NoError(t, err)

	// Run server
	var (
		interruptCh = make(chan error, 1)
		bootstrapCh = make(chan struct{}, 1)
		serverWg    sync.WaitGroup
	)
	serverWg.Add(1)
	go func() {
		server.Run(server.RunOptions{
			ConfigFile:  configFd.Name(),
			BootstrapCh: bootstrapCh,
			InterruptCh: interruptCh,
		})
		serverWg.Done()
	}()
	defer func() {
		// Resetting DefaultServeMux to prevent multiple assignments
		// to /debug/dump in Server.Run()
		http.DefaultServeMux = http.NewServeMux()
	}()

	// Wait for bootstrap
	<-bootstrapCh

	// Create client, read and write some data
	// NB(r): Make sure client config points to the root config
	// service since we're going to instantiate the client configuration
	// just by itself.
	cfg.DB.Client.EnvironmentConfig = &envCfg

	cli, err := cfg.DB.Client.NewClient(client.ConfigurationParameters{})
	require.NoError(t, err)

	adminCli := cli.(client.AdminClient)
	adminSession, err := adminCli.DefaultAdminSession()
	require.NoError(t, err)
	defer adminSession.Close()

	// Propagation of shard state from Initializing --> Available post-bootstrap is eventually
	// consistent, so we must wait.
	waitUntilAllShardsAreAvailable(t, adminSession)

	// Cast to narrower-interface instead of grabbing DefaultSession to make sure
	// we use the same topology.Map that we validated in waitUntilAllShardsAreAvailable.
	session := adminSession.(client.Session)
	defer session.Close()

	start := xtime.Now().Add(-time.Minute)
	values := []struct {
		value float64
		at    xtime.UnixNano
		unit  xtime.Unit
	}{
		{value: 1.0, at: start, unit: xtime.Second},
		{value: 2.0, at: start.Add(1 * time.Second), unit: xtime.Second},
		{value: 3.0, at: start.Add(2 * time.Second), unit: xtime.Second},
	}

	// unknown ns test
	randomNs := "random-ns"
	ctx := xcontext.NewBackground()
	for _, v := range values {
		err := session.WriteTagged(ctx, ident.StringID(randomNs),
			ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(
				ident.StringTag("foo", "bar"),
				ident.StringTag("baz", "foo"),
			)),
			v.at, v.value, v.unit, nil)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), dberrors.NewUnknownNamespaceError(randomNs).Error()))
	}

	for _, v := range values {
		err := session.WriteTagged(ctx, ident.StringID(namespaceID),
			ident.StringID("foo"),
			ident.NewTagsIterator(ident.NewTags(
				ident.StringTag("foo", "bar"),
				ident.StringTag("baz", "foo"),
			)),
			v.at, v.value, v.unit, nil)
		require.NoError(t, err)
	}

	// Account for first value inserted at xtime.Second precision
	fetchStart := start.Truncate(time.Second)

	// Account for last value being inserted at xtime.Second and
	// the "end" param to fetch being exclusive
	fetchEnd := values[len(values)-1].at.Truncate(time.Second).Add(time.Nanosecond)

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	assert.NoError(t, err)

	goCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	iters, fetchResponse, err := session.FetchTagged(goCtx,
		ident.StringID(namespaceID),
		index.Query{reQuery},
		index.QueryOptions{
			StartInclusive: fetchStart,
			EndExclusive:   fetchEnd,
		})
	assert.NoError(t, err)
	assert.True(t, fetchResponse.Exhaustive)
	assert.Equal(t, 1, iters.Len())
	iter := iters.Iters()[0]
	assert.Equal(t, namespaceID, iter.Namespace().String())
	assert.Equal(t, "foo", iter.ID().String())
	assert.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("foo", "bar", "baz", "foo")).Matches(iter.Tags()))
	for _, v := range values {
		require.True(t, iter.Next())
		dp, unit, _ := iter.Current()
		assert.Equal(t, v.value, dp.Value)
		// Account for xtime.Second precision on values going in
		expectAt := v.at.Truncate(time.Second)
		assert.Equal(t, expectAt, dp.TimestampNanos)
		assert.Equal(t, v.unit, unit)
	}

	resultsIter, resultsFetchResponse, err := session.FetchTaggedIDs(goCtx,
		ident.StringID(namespaceID),
		index.Query{reQuery},
		index.QueryOptions{
			StartInclusive: fetchStart,
			EndExclusive:   fetchEnd,
		})
	assert.NoError(t, err)
	assert.True(t, resultsFetchResponse.Exhaustive)
	assert.True(t, resultsIter.Next())
	nsID, tsID, tags := resultsIter.Current()
	assert.Equal(t, namespaceID, nsID.String())
	assert.Equal(t, "foo", tsID.String())
	assert.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("foo", "bar", "baz", "foo")).Matches(tags))
	assert.False(t, resultsIter.Next())
	assert.NoError(t, resultsIter.Err())

	// Wait for server to stop
	interruptCh <- fmt.Errorf("test complete")
	serverWg.Wait()
}

var indexTestConfig = `
db:
    logging:
        level: info
        file: {{.LogFile}}

    metrics:
        prometheus:
            handlerPath: /metrics
            listenAddress: 0.0.0.0:10005
            onError: none
        sanitization: prometheus
        samplingRate: 1.0
        extended: detailed

    listenAddress: 0.0.0.0:{{.ServicePort}}
    clusterListenAddress: 0.0.0.0:10001
    httpNodeListenAddress: 0.0.0.0:10002
    httpClusterListenAddress: 0.0.0.0:10003
    debugListenAddress: 0.0.0.0:10004

    hostID:
        resolver: config
        value: {{.HostID}}

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

    gcPercentage: 100

    writeNewSeriesAsync: false
    writeNewSeriesBackoffDuration: 2ms

    commitlog:
        flushMaxBytes: 524288
        flushEvery: 1s
        queue:
            calculationType: fixed
            size: 2097152

    filesystem:
        filePathPrefix: {{.DataDir}}
        writeBufferSize: 65536
        dataReadBufferSize: 65536
        infoReadBufferSize: 128
        seekReadBufferSize: 4096
        throughputLimitMbps: 100.0
        throughputCheckEvery: 128

    repair:
        enabled: false
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
        replicaMetadataSlicePool:
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

    discovery:
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
