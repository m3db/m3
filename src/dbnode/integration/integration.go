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

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	persistfs "github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	bfs "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/uninitialized"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/topology/testutil"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/instrument"
	xretry "github.com/m3db/m3/src/x/retry"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
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

func newMultiAddrTestOptions(opts TestOptions, instance int) TestOptions {
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
	customOpts ...client.CustomAdminOption,
) client.AdminClient {
	if adminOpts == nil {
		adminOpts = client.NewAdminOptions()
	}

	adminOpts = adminOpts.
		SetOrigin(origin).
		SetInstrumentOptions(instrumentOpts).
		SetClusterConnectConsistencyLevel(topology.ConnectConsistencyLevelAny).
		SetTopologyInitializer(topologyInitializer).
		SetClusterConnectTimeout(time.Second).(client.AdminOptions)

	for _, o := range customOpts {
		adminOpts = o(adminOpts)
	}

	adminClient, err := client.NewAdminClient(adminOpts)
	require.NoError(t, err)

	return adminClient
}

// BootstrappableTestSetupOptions defines options for test setups.
type BootstrappableTestSetupOptions struct {
	FinalBootstrapper           string
	BootstrapBlocksBatchSize    int
	BootstrapBlocksConcurrency  int
	BootstrapConsistencyLevel   topology.ReadConsistencyLevel
	TopologyInitializer         topology.Initializer
	TestStatsReporter           xmetrics.TestStatsReporter
	DisablePeersBootstrapper    bool
	UseTChannelClientForWriting bool
	EnableRepairs               bool
	AdminClientCustomOpts       []client.CustomAdminOption
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

// NewDefaultBootstrappableTestSetups creates dbnode test setups.
func NewDefaultBootstrappableTestSetups( // nolint:gocyclo
	t *testing.T,
	opts TestOptions,
	setupOpts []BootstrappableTestSetupOptions,
) (testSetups, closeFn) {
	var (
		replicas        = len(setupOpts)
		setups          []TestSetup
		cleanupFns      []func()
		cleanupFnsMutex sync.RWMutex

		appendCleanupFn = func(fn func()) {
			cleanupFnsMutex.Lock()
			defer cleanupFnsMutex.Unlock()
			cleanupFns = append(cleanupFns, fn)
		}
	)

	shardSet, err := newTestShardSet(opts.NumShards())
	require.NoError(t, err)
	for i := 0; i < replicas; i++ {
		var (
			instance                    = i
			usingPeersBootstrapper      = !setupOpts[i].DisablePeersBootstrapper
			finalBootstrapperToUse      = setupOpts[i].FinalBootstrapper
			useTChannelClientForWriting = setupOpts[i].UseTChannelClientForWriting
			bootstrapBlocksBatchSize    = setupOpts[i].BootstrapBlocksBatchSize
			bootstrapBlocksConcurrency  = setupOpts[i].BootstrapBlocksConcurrency
			bootstrapConsistencyLevel   = setupOpts[i].BootstrapConsistencyLevel
			topologyInitializer         = setupOpts[i].TopologyInitializer
			testStatsReporter           = setupOpts[i].TestStatsReporter
			enableRepairs               = setupOpts[i].EnableRepairs
			origin                      topology.Host
			instanceOpts                = newMultiAddrTestOptions(opts, instance)
			adminClientCustomOpts       = setupOpts[i].AdminClientCustomOpts
		)

		if finalBootstrapperToUse == "" {
			finalBootstrapperToUse = bootstrapper.NoOpAllBootstrapperName
		}

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
			SetClusterDatabaseTopologyInitializer(topologyInitializer).
			SetUseTChannelClientForWriting(useTChannelClientForWriting)

		if i > 0 {
			// NB(bodu): Need to reset the global counter of number of index
			// claim manager instances after the initial node.
			persistfs.ResetIndexClaimsManagersUnsafe()
		}
		setup, err := NewTestSetup(t, instanceOpts, nil, opts.StorageOptsFn())
		require.NoError(t, err)
		topologyInitializer = setup.TopologyInitializer()

		instrumentOpts := setup.StorageOpts().InstrumentOptions()
		logger := instrumentOpts.Logger()
		logger = logger.With(zap.Int("instance", instance))
		instrumentOpts = instrumentOpts.SetLogger(logger)
		if testStatsReporter != nil {
			scope, _ := tally.NewRootScope(tally.ScopeOptions{Reporter: testStatsReporter}, 100*time.Millisecond)
			instrumentOpts = instrumentOpts.SetMetricsScope(scope)
		}
		setup.SetStorageOpts(setup.StorageOpts().SetInstrumentOptions(instrumentOpts))

		var (
			bsOpts            = newDefaulTestResultOptions(setup.StorageOpts())
			finalBootstrapper bootstrap.BootstrapperProvider

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
			uninitialized.NewUninitializedTopologyBootstrapperProvider(
				uninitialized.NewOptions().
					SetInstrumentOptions(instrumentOpts), nil)
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
			t, adminOpts, topologyInitializer, origin, instrumentOpts, adminClientCustomOpts...)
		setup.SetStorageOpts(setup.StorageOpts().SetAdminClient(adminClient))

		storageIdxOpts := setup.StorageOpts().IndexOptions()
		fsOpts := setup.StorageOpts().CommitLogOptions().FilesystemOptions()
		if usingPeersBootstrapper {
			var (
				runtimeOptsMgr = setup.StorageOpts().RuntimeOptionsManager()
				runtimeOpts    = runtimeOptsMgr.Get().
						SetClientBootstrapConsistencyLevel(bootstrapConsistencyLevel)
			)
			runtimeOptsMgr.Update(runtimeOpts)

			peersOpts := peers.NewOptions().
				SetResultOptions(bsOpts).
				SetAdminClient(adminClient).
				SetIndexOptions(storageIdxOpts).
				SetFilesystemOptions(fsOpts).
				// PersistManager need to be set or we will never execute
				// the persist bootstrapping path
				SetPersistManager(setup.StorageOpts().PersistManager()).
				SetIndexClaimsManager(setup.StorageOpts().IndexClaimsManager()).
				SetCompactor(newCompactor(t, storageIdxOpts)).
				SetRuntimeOptionsManager(runtimeOptsMgr).
				SetContextPool(setup.StorageOpts().ContextPool())

			finalBootstrapper, err = peers.NewPeersBootstrapperProvider(peersOpts, finalBootstrapper)
			require.NoError(t, err)
		}

		persistMgr, err := persistfs.NewPersistManager(fsOpts)
		require.NoError(t, err)

		bfsOpts := bfs.NewOptions().
			SetResultOptions(bsOpts).
			SetFilesystemOptions(fsOpts).
			SetIndexOptions(storageIdxOpts).
			SetCompactor(newCompactor(t, storageIdxOpts)).
			SetPersistManager(persistMgr).
			SetIndexClaimsManager(setup.StorageOpts().IndexClaimsManager())

		fsBootstrapper, err := bfs.NewFileSystemBootstrapperProvider(bfsOpts, finalBootstrapper)
		require.NoError(t, err)

		processOpts := bootstrap.NewProcessOptions().
			SetTopologyMapProvider(setup).
			SetOrigin(setup.Origin())
		provider, err := bootstrap.NewProcessProvider(fsBootstrapper, processOpts, bsOpts, fsOpts)
		require.NoError(t, err)

		setup.SetStorageOpts(setup.StorageOpts().SetBootstrapProcessProvider(provider))

		if enableRepairs {
			setup.SetStorageOpts(setup.StorageOpts().
				SetRepairEnabled(true).
				SetRepairOptions(
					setup.StorageOpts().RepairOptions().
						SetRepairThrottle(time.Millisecond).
						SetRepairCheckInterval(time.Millisecond).
						SetAdminClients([]client.AdminClient{adminClient}).
						SetDebugShadowComparisonsPercentage(1.0).
						// Avoid log spam.
						SetDebugShadowComparisonsEnabled(false)))
		}

		setups = append(setups, setup)
		appendCleanupFn(func() {
			setup.Close()
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

func writeTestDataToDisk(
	metadata namespace.Metadata,
	setup TestSetup,
	seriesMaps generate.SeriesBlocksByStart,
	volume int,
) error {
	ropts := metadata.Options().RetentionOptions()
	writer := generate.NewWriter(setup.GeneratorOptions(ropts))
	return writer.WriteData(namespace.NewContextFrom(metadata), setup.ShardSet(), seriesMaps, volume)
}

func writeTestSnapshotsToDiskWithPredicate(
	metadata namespace.Metadata,
	setup TestSetup,
	seriesMaps generate.SeriesBlocksByStart,
	volume int,
	pred generate.WriteDatapointPredicate,
	snapshotInterval time.Duration,
) error {
	ropts := metadata.Options().RetentionOptions()
	writer := generate.NewWriter(setup.GeneratorOptions(ropts))
	return writer.WriteSnapshotWithPredicate(
		namespace.NewContextFrom(metadata), setup.ShardSet(), seriesMaps, volume, pred, snapshotInterval)
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

func newCompactor(
	t *testing.T,
	opts index.Options,
) *compaction.Compactor {
	compactor, err := newCompactorWithErr(opts)
	require.NoError(t, err)
	return compactor
}

func newCompactorWithErr(opts index.Options) (*compaction.Compactor, error) {
	return compaction.NewCompactor(opts.MetadataArrayPool(),
		index.MetadataArrayPoolCapacity,
		opts.SegmentBuilderOptions(),
		opts.FSTSegmentOptions(),
		compaction.CompactorOptions{
			FSTWriterOptions: &fst.WriterOptions{
				// DisableRegistry is set to true to trade a larger FST size
				// for a faster FST compaction since we want to reduce the end
				// to end latency for time to first index a metric.
				DisableRegistry: true,
			},
		})
}

func writeTestIndexDataToDisk(
	md namespace.Metadata,
	storageOpts storage.Options,
	indexVolumeType idxpersist.IndexVolumeType,
	blockStart time.Time,
	shards []uint32,
	docs []doc.Metadata,
) error {
	blockSize := md.Options().IndexOptions().BlockSize()
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	writer, err := fs.NewIndexWriter(fsOpts)
	if err != nil {
		return err
	}
	segmentWriter, err := idxpersist.NewMutableSegmentFileSetWriter(fst.WriterOptions{})
	if err != nil {
		return err
	}

	shardsMap := make(map[uint32]struct{})
	for _, shard := range shards {
		shardsMap[shard] = struct{}{}
	}
	volumeIndex, err := fs.NextIndexFileSetVolumeIndex(
		fsOpts.FilePathPrefix(),
		md.ID(),
		blockStart,
	)
	if err != nil {
		return err
	}
	writerOpts := fs.IndexWriterOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   md.ID(),
			BlockStart:  blockStart,
			VolumeIndex: volumeIndex,
		},
		BlockSize:       blockSize,
		Shards:          shardsMap,
		IndexVolumeType: indexVolumeType,
	}
	if err := writer.Open(writerOpts); err != nil {
		return err
	}

	builder, err := builder.NewBuilderFromDocuments(builder.NewOptions())
	for _, doc := range docs {
		_, err = builder.Insert(doc)
		if err != nil {
			return err
		}
	}

	if err := segmentWriter.Reset(builder); err != nil {
		return err
	}
	if err := writer.WriteSegmentFileSet(segmentWriter); err != nil {
		return err
	}
	if err := builder.Close(); err != nil {
		return err
	}
	return writer.Close()
}
