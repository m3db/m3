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

	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	bfs "github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/topology"
	xmetrics "github.com/m3db/m3db/x/metrics"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

const (
	multiAddrPortStart = 9000
	multiAddrPortEach  = 4
)

// TODO: refactor and use m3x/clock ...
type conditionFn func() bool

func waitUntil(fn conditionFn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func newMultiAddrTestOptions(opts testOptions, instance int) testOptions {
	bind := "0.0.0.0"
	start := multiAddrPortStart + (instance * multiAddrPortEach)
	return opts.
		SetID(fmt.Sprintf("testhost%d", instance)).
		SetTChannelNodeAddr(fmt.Sprintf("%s:%d", bind, start)).
		SetTChannelClusterAddr(fmt.Sprintf("%s:%d", bind, start+1)).
		SetHTTPNodeAddr(fmt.Sprintf("%s:%d", bind, start+2)).
		SetHTTPClusterAddr(fmt.Sprintf("%s:%d", bind, start+3))
}

func newMultiAddrAdminClient(
	t *testing.T,
	adminOpts client.AdminOptions,
	instrumentOpts instrument.Options,
	shardSet sharding.ShardSet,
	replicas int,
	instance int,
) client.AdminClient {
	if adminOpts == nil {
		adminOpts = client.NewAdminOptions()
	}

	var (
		start         = multiAddrPortStart
		hostShardSets []topology.HostShardSet
		origin        topology.Host
	)
	for i := 0; i < replicas; i++ {
		id := fmt.Sprintf("testhost%d", i)
		nodeAddr := fmt.Sprintf("127.0.0.1:%d", start+(i*multiAddrPortEach))
		host := topology.NewHost(id, nodeAddr)
		if i == instance {
			origin = host
		}
		hostShardSet := topology.NewHostShardSet(host, shardSet)
		hostShardSets = append(hostShardSets, hostShardSet)
	}

	staticOptions := topology.NewStaticOptions().
		SetShardSet(shardSet).
		SetReplicas(replicas).
		SetHostShardSets(hostShardSets)

	clientOpts := adminOpts.
		SetOrigin(origin).
		SetInstrumentOptions(instrumentOpts).
		SetTopologyInitializer(topology.NewStaticInitializer(staticOptions)).
		SetClusterConnectConsistencyLevel(client.ConnectConsistencyLevelAny).
		SetClusterConnectTimeout(time.Second)

	adminClient, err := client.NewAdminClient(clientOpts.(client.AdminOptions))
	require.NoError(t, err)

	return adminClient
}

type bootstrappableTestSetupOptions struct {
	disablePeersBootstrapper   bool
	useV2PeerBootstrapper      bool
	bootstrapBlocksBatchSize   int
	bootstrapBlocksConcurrency int
	topologyInitializer        topology.Initializer
	testStatsReporter          xmetrics.TestStatsReporter
}

type closeFn func()

func newDefaulTestResultOptions(
	storageOpts storage.Options,
) result.Options {
	return result.NewOptions().
		SetClockOptions(storageOpts.ClockOptions()).
		SetInstrumentOptions(storageOpts.InstrumentOptions()).
		SetDatabaseBlockOptions(storageOpts.DatabaseBlockOptions())
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
	)
	appendCleanupFn := func(fn func()) {
		cleanupFnsMutex.Lock()
		defer cleanupFnsMutex.Unlock()
		cleanupFns = append(cleanupFns, fn)
	}
	for i := 0; i < replicas; i++ {
		var (
			instance                   = i
			usingPeersBoostrapper      = !setupOpts[i].disablePeersBootstrapper
			bootstrapBlocksBatchSize   = setupOpts[i].bootstrapBlocksBatchSize
			bootstrapBlocksConcurrency = setupOpts[i].bootstrapBlocksConcurrency
			topologyInitializer        = setupOpts[i].topologyInitializer
			testStatsReporter          = setupOpts[i].testStatsReporter
			instanceOpts               = newMultiAddrTestOptions(opts, instance)
		)

		if topologyInitializer != nil {
			instanceOpts = instanceOpts.
				SetClusterDatabaseTopologyInitializer(topologyInitializer)
		}

		setup, err := newTestSetup(t, instanceOpts)
		require.NoError(t, err)

		instrumentOpts := setup.storageOpts.InstrumentOptions()
		logger := instrumentOpts.Logger()
		logger = logger.WithFields(xlog.NewField("instance", instance))
		instrumentOpts = instrumentOpts.SetLogger(logger)
		if testStatsReporter != nil {
			scope, _ := tally.NewRootScope(tally.ScopeOptions{Reporter: testStatsReporter}, 100*time.Millisecond)
			instrumentOpts = instrumentOpts.SetMetricsScope(scope)
		}
		setup.storageOpts = setup.storageOpts.SetInstrumentOptions(instrumentOpts)

		bsOpts := newDefaulTestResultOptions(setup.storageOpts)
		noOpAll := bootstrapper.NewNoOpAllBootstrapper()
		var peersBootstrapper bootstrap.Bootstrapper

		if usingPeersBoostrapper {
			adminOpts := client.NewAdminOptions()
			if bootstrapBlocksBatchSize > 0 {
				adminOpts = adminOpts.SetFetchSeriesBlocksBatchSize(bootstrapBlocksBatchSize)
			}
			if bootstrapBlocksConcurrency > 0 {
				adminOpts = adminOpts.SetFetchSeriesBlocksBatchConcurrency(bootstrapBlocksConcurrency)
			}
			// Prevent integration tests from timing out when a node is down
			adminOpts = adminOpts.SetFetchSeriesBlocksMetadataBatchInitialBackoff(10 * time.Millisecond)
			adminClient := newMultiAddrAdminClient(
				t, adminOpts, instrumentOpts, setup.shardSet, replicas, instance)
			peersOpts := peers.NewOptions().
				SetResultOptions(bsOpts).
				SetAdminClient(adminClient).
				SetUseV2(setupOpts[i].useV2PeerBootstrapper)

			peersBootstrapper, err = peers.NewPeersBootstrapper(peersOpts, noOpAll)
			require.NoError(t, err)
		} else {
			peersBootstrapper = noOpAll
		}

		fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
		filePathPrefix := fsOpts.FilePathPrefix()
		bfsOpts := bfs.NewOptions().
			SetResultOptions(bsOpts).
			SetFilesystemOptions(fsOpts)

		fsBootstrapper := bfs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, peersBootstrapper)
		setup.storageOpts = setup.storageOpts.
			SetBootstrapProcess(bootstrap.NewProcess(fsBootstrapper, bsOpts))

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

// nolint: deadcode
func writeTestDataToDisk(
	metadata namespace.Metadata,
	setup *testSetup,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) error {
	ropts := metadata.Options().RetentionOptions()
	writer := generate.NewWriter(setup.generatorOptions(ropts))
	return writer.Write(metadata.ID(), setup.shardSet, seriesMaps)
}

// nolint: deadcode
func concatShards(a, b shard.Shards) shard.Shards {
	all := append(a.All(), b.All()...)
	return shard.NewShards(all)
}

func newShardsRange(from, to uint32) []uint32 {
	var ids []uint32
	for i := from; i <= to; i++ {
		ids = append(ids, i)
	}
	return ids
}

func newClusterShards(ids []uint32, s shard.State) shard.Shards {
	var shards []shard.Shard
	for _, id := range ids {
		shards = append(shards, shard.NewShard(id).SetState(s))
	}
	return shard.NewShards(shards)
}

// nolint: deadcode
func newClusterShardsRange(from, to uint32, s shard.State) shard.Shards {
	return newClusterShards(newShardsRange(from, to), s)
}

// nolint: deadcode
func newClusterEmptyShardsRange() shard.Shards {
	return newClusterShards(nil, shard.Available)
}

// nolint: deadcode
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
