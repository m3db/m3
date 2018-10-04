// Copyright (c) 2016 Uber Technologies, Inc.
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

package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	persistfs "github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	bfs "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/uninitialized"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/topology/testutil"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xretry "github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	multiAddrPortStart = 9000
	multiAddrPortEach  = 5
)

// TODO: refactor and use m3x/clock ...
type conditionFn func() bool

func waitUntil(fn conditionFn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func newMultiAddrTestOptions(opts testOptions, instance int) testOptions {
	bind := "127.0.0.1"
	start := multiAddrPortStart + (instance * multiAddrPortEach)
	return opts.
		SetID(fmt.Sprintf("testhost%d", instance)).
		SetTChannelNodeAddr(fmt.Sprintf("%s:%d", bind, start)).
		SetTChannelClusterAddr(fmt.Sprintf("%s:%d", bind, start+1)).
		SetHTTPNodeAddr(fmt.Sprintf("%s:%d", bind, start+2)).
		SetHTTPClusterAddr(fmt.Sprintf("%s:%d", bind, start+3)).
		SetHTTPDebugAddr(fmt.Sprintf("%s:%d", bind, start+4))
}

func newMultiAddrAdminClient(
	t *testing.T,
	adminOpts client.AdminOptions,
	topologyInitializer topology.Initializer,
	origin topology.Host,
	instrumentOpts instrument.Options,
) client.AdminClient {
	if adminOpts == nil {
		adminOpts = client.NewAdminOptions()
	}

	clientOpts := adminOpts.
		SetOrigin(origin).
		SetInstrumentOptions(instrumentOpts).
		SetClusterConnectConsistencyLevel(topology.ConnectConsistencyLevelAny).
		SetClusterConnectTimeout(time.Second)

	clientOpts = clientOpts.SetTopologyInitializer(topologyInitializer)

	adminClient, err := client.NewAdminClient(clientOpts.(client.AdminOptions))
	require.NoError(t, err)

	return adminClient
}

type bootstrappableTestSetupOptions struct {
	disablePeersBootstrapper           bool
	finalBootstrapper                  string
	fetchBlocksMetadataEndpointVersion client.FetchBlocksMetadataEndpointVersion
	bootstrapBlocksBatchSize           int
	bootstrapBlocksConcurrency         int
	bootstrapConsistencyLevel          topology.ReadConsistencyLevel
	topologyInitializer                topology.Initializer
	testStatsReporter                  xmetrics.TestStatsReporter
}

type closeFn func()

func newDefaulTestResultOptions(
	storageOpts storage.Options,
) result.Options {
	return result.NewOptions().
		SetClockOptions(storageOpts.ClockOptions()).
		SetInstrumentOptions(storageOpts.InstrumentOptions()).
		SetDatabaseBlockOptions(storageOpts.DatabaseBlockOptions()).
		SetSeriesCachePolicy(storageOpts.SeriesCachePolicy())
}

func newDefaultBootstrappableTestSetups(
	t *testing.T,
	opts testOptions,
	setupOpts []bootstrappableTestSetupOptions,
) (testSetups, closeFn) {
	var (
		replicas        = len(setupOpts)
		setups          []*testSetup
		cleanupFns      []func()
		cleanupFnsMutex sync.RWMutex

		appendCleanupFn = func(fn func()) {
			cleanupFnsMutex.Lock()
			defer cleanupFnsMutex.Unlock()
			cleanupFns = append(cleanupFns, fn)
		}
		anySetupUsingFetchBlocksMetadataEndpointV1 = false
	)

	for i := range setupOpts {
		v1 := client.FetchBlocksMetadataEndpointV1
		if setupOpts[i].fetchBlocksMetadataEndpointVersion == v1 {
			anySetupUsingFetchBlocksMetadataEndpointV1 = true
			break
		}
	}

	shardSet, err := newTestShardSet(opts.NumShards())
	require.NoError(t, err)
	for i := 0; i < replicas; i++ {
		var (
			instance                   = i
			usingPeersBootstrapper     = !setupOpts[i].disablePeersBootstrapper
			finalBootstrapperToUse     = setupOpts[i].finalBootstrapper
			bootstrapBlocksBatchSize   = setupOpts[i].bootstrapBlocksBatchSize
			bootstrapBlocksConcurrency = setupOpts[i].bootstrapBlocksConcurrency
			bootstrapConsistencyLevel  = setupOpts[i].bootstrapConsistencyLevel
			topologyInitializer        = setupOpts[i].topologyInitializer
			testStatsReporter          = setupOpts[i].testStatsReporter
			origin                     topology.Host
			instanceOpts               = newMultiAddrTestOptions(opts, instance)
		)

		if topologyInitializer == nil {
			// Setup static topology initializer
			var (
				start         = multiAddrPortStart
				hostShardSets []topology.HostShardSet
			)

			for i := 0; i < replicas; i++ {
				id := fmt.Sprintf("testhost%d", i)
				nodeAddr := fmt.Sprintf("127.0.0.1:%d", start+(i*multiAddrPortEach))
				host := topology.NewHost(id, nodeAddr)
				if i == instance {
					origin = host
				}
				shardSet, err := newTestShardSet(opts.NumShards())
				require.NoError(t, err)
				hostShardSet := topology.NewHostShardSet(host, shardSet)
				hostShardSets = append(hostShardSets, hostShardSet)
			}

			staticOptions := topology.NewStaticOptions().
				SetShardSet(shardSet).
				SetReplicas(replicas).
				SetHostShardSets(hostShardSets)
			topologyInitializer = topology.NewStaticInitializer(staticOptions)
		}

		instanceOpts = instanceOpts.
			SetClusterDatabaseTopologyInitializer(topologyInitializer)

		setup, err := newTestSetup(t, instanceOpts, nil)
		require.NoError(t, err)
		topologyInitializer = setup.topoInit

		// Force correct series cache policy if using V1 version
		// TODO: Remove once v1 endpoint is gone
		if anySetupUsingFetchBlocksMetadataEndpointV1 {
			setup.storageOpts = setup.storageOpts.SetSeriesCachePolicy(series.CacheAll)
		}

		instrumentOpts := setup.storageOpts.InstrumentOptions()
		logger := instrumentOpts.Logger()
		logger = logger.WithFields(xlog.NewField("instance", instance))
		instrumentOpts = instrumentOpts.SetLogger(logger)
		if testStatsReporter != nil {
			scope, _ := tally.NewRootScope(tally.ScopeOptions{Reporter: testStatsReporter}, 100*time.Millisecond)
			instrumentOpts = instrumentOpts.SetMetricsScope(scope)
		}
		setup.storageOpts = setup.storageOpts.SetInstrumentOptions(instrumentOpts)

		var (
			bsOpts            = newDefaulTestResultOptions(setup.storageOpts)
			finalBootstrapper = uninitialized.NewuninitializedTopologyBootstrapperProvider(
				uninitialized.NewOptions().
					SetInstrumentOptions(instrumentOpts), nil)

			adminOpts = client.NewAdminOptions().
					SetTopologyInitializer(topologyInitializer).(client.AdminOptions).
					SetOrigin(origin)

				// Prevent integration tests from timing out when a node is down
			retryOpts = xretry.NewOptions().
					SetInitialBackoff(1 * time.Millisecond).
					SetMaxRetries(1).
					SetJitter(true)
			retrier = xretry.NewRetrier(retryOpts)
		)

		switch finalBootstrapperToUse {
		case bootstrapper.NoOpAllBootstrapperName:
			finalBootstrapper = bootstrapper.NewNoOpAllBootstrapperProvider()
		case uninitialized.UninitializedTopologyBootstrapperName:
			// Default already configured, do nothing
		default:
			panic(fmt.Sprintf(
				"Unknown final bootstrapper to use: %v", finalBootstrapperToUse))
		}

		if bootstrapBlocksBatchSize > 0 {
			adminOpts = adminOpts.SetFetchSeriesBlocksBatchSize(bootstrapBlocksBatchSize)
		}
		if bootstrapBlocksConcurrency > 0 {
			adminOpts = adminOpts.SetFetchSeriesBlocksBatchConcurrency(bootstrapBlocksConcurrency)
		}
		adminOpts = adminOpts.SetStreamBlocksRetrier(retrier)

		adminClient := newMultiAddrAdminClient(
			t, adminOpts, topologyInitializer, origin, instrumentOpts)
		if usingPeersBootstrapper {
			var (
				runtimeOptsMgr = setup.storageOpts.RuntimeOptionsManager()
				runtimeOpts    = runtimeOptsMgr.Get().
						SetClientBootstrapConsistencyLevel(bootstrapConsistencyLevel)
			)
			runtimeOptsMgr.Update(runtimeOpts)

			peersOpts := peers.NewOptions().
				SetResultOptions(bsOpts).
				SetAdminClient(adminClient).
				SetFetchBlocksMetadataEndpointVersion(setupOpts[i].fetchBlocksMetadataEndpointVersion).
				// DatabaseBlockRetrieverManager and PersistManager need to be set or we will never execute
				// the persist bootstrapping path
				SetDatabaseBlockRetrieverManager(setup.storageOpts.DatabaseBlockRetrieverManager()).
				SetPersistManager(setup.storageOpts.PersistManager()).
				SetRuntimeOptionsManager(runtimeOptsMgr)

			finalBootstrapper, err = peers.NewPeersBootstrapperProvider(peersOpts, finalBootstrapper)
			require.NoError(t, err)
		}

		fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
		persistMgr, err := persistfs.NewPersistManager(fsOpts)
		require.NoError(t, err)

		bfsOpts := bfs.NewOptions().
			SetResultOptions(bsOpts).
			SetFilesystemOptions(fsOpts).
			SetDatabaseBlockRetrieverManager(setup.storageOpts.DatabaseBlockRetrieverManager()).
			SetPersistManager(persistMgr)

		fsBootstrapper, err := bfs.NewFileSystemBootstrapperProvider(bfsOpts, finalBootstrapper)
		require.NoError(t, err)

		processOpts := bootstrap.NewProcessOptions().
			SetTopologyMapProvider(&latestDBTopoMapProvider{setup}).
			SetOrigin(setup.origin)
		provider, err := bootstrap.NewProcessProvider(fsBootstrapper, processOpts, bsOpts)
		require.NoError(t, err)

		setup.storageOpts = setup.storageOpts.SetBootstrapProcessProvider(provider)

		setups = append(setups, setup)
		appendCleanupFn(func() {
			setup.close()
		})
	}

	return setups, func() {
		cleanupFnsMutex.RLock()
		defer cleanupFnsMutex.RUnlock()
		for _, fn := range cleanupFns {
			fn()
		}
	}
}

// latestDBTopoMapProvider makes sure that the topology map provided always
// comes from the most recent database in the testSetup since they get
// recreated everytime startServer/stopServer is called.
type latestDBTopoMapProvider struct {
	setup *testSetup
}

func (d *latestDBTopoMapProvider) TopologyMap() (topology.Map, error) {
	return d.setup.db.TopologyMap()
}

func writeTestDataToDisk(
	metadata namespace.Metadata,
	setup *testSetup,
	seriesMaps generate.SeriesBlocksByStart,
) error {
	ropts := metadata.Options().RetentionOptions()
	writer := generate.NewWriter(setup.generatorOptions(ropts))
	return writer.WriteData(metadata.ID(), setup.shardSet, seriesMaps)
}

func writeTestSnapshotsToDiskWithPredicate(
	metadata namespace.Metadata,
	setup *testSetup,
	seriesMaps generate.SeriesBlocksByStart,
	pred generate.WriteDatapointPredicate,
	snapshotInterval time.Duration,
) error {
	ropts := metadata.Options().RetentionOptions()
	writer := generate.NewWriter(setup.generatorOptions(ropts))
	return writer.WriteSnapshotWithPredicate(
		metadata.ID(), setup.shardSet, seriesMaps, pred, snapshotInterval)
}

func concatShards(a, b shard.Shards) shard.Shards {
	all := append(a.All(), b.All()...)
	return shard.NewShards(all)
}

func newClusterShardsRange(from, to uint32, s shard.State) shard.Shards {
	return shard.NewShards(testutil.ShardsRange(from, to, s))
}

func newClusterEmptyShardsRange() shard.Shards {
	return shard.NewShards(testutil.Shards(nil, shard.Available))
}

func waitUntilHasBootstrappedShardsExactly(
	db storage.Database,
	shards []uint32,
) {
	for {
		if hasBootstrappedShardsExactly(db, shards) {
			return
		}
		time.Sleep(time.Second)
	}
}

func hasBootstrappedShardsExactly(
	db storage.Database,
	shards []uint32,
) bool {
	for _, namespace := range db.Namespaces() {
		expect := make(map[uint32]struct{})
		pending := make(map[uint32]struct{})
		for _, shard := range shards {
			expect[shard] = struct{}{}
			pending[shard] = struct{}{}
		}

		for _, s := range namespace.Shards() {
			if _, ok := expect[s.ID()]; !ok {
				// Not expecting shard
				return false
			}
			if s.IsBootstrapped() {
				delete(pending, s.ID())
			}
		}

		if len(pending) != 0 {
			// Not all shards bootstrapped
			return false
		}
	}

	return true
}
