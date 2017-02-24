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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	bfs "github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	xmetrics "github.com/m3db/m3db/x/metrics"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
	"github.com/uber-go/tally"

	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/require"
)

const (
	multiAddrPortStart = 9000
	multiAddrPortEach  = 4
)

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

func newTestSleepFn() (func(d time.Duration), func(overrideFn func(d time.Duration))) {
	var mutex sync.RWMutex
	sleepFn := time.Sleep
	return func(d time.Duration) {
			mutex.RLock()
			defer mutex.RUnlock()
			sleepFn(d)
		}, func(fn func(d time.Duration)) {
			mutex.Lock()
			defer mutex.Unlock()
			sleepFn = fn
		}
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
	bootstrapBlocksBatchSize   int
	bootstrapBlocksConcurrency int
	topologyInitializer        topology.Initializer
	testStatsReporter          xmetrics.TestStatsReporter
}

type closeFn func()

func newDefaulTestResultOptions(
	storageOpts storage.Options,
	instrumentOpts instrument.Options,
) result.Options {
	return result.NewOptions().
		SetClockOptions(storageOpts.ClockOptions()).
		SetInstrumentOptions(instrumentOpts).
		SetRetentionOptions(storageOpts.RetentionOptions()).
		SetDatabaseBlockOptions(storageOpts.DatabaseBlockOptions())
}

func newDefaultBootstrappableTestSetups(
	t *testing.T,
	opts testOptions,
	retentionOpts retention.Options,
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

		setup, err := newTestSetup(instanceOpts)
		require.NoError(t, err)

		setup.storageOpts = setup.storageOpts.
			SetRetentionOptions(retentionOpts)

		instrumentOpts := setup.storageOpts.InstrumentOptions()

		bsOpts := newDefaulTestResultOptions(setup.storageOpts, instrumentOpts)
		noOpAll := bootstrapper.NewNoOpAllBootstrapper()

		var adminClient client.AdminClient
		if usingPeersBoostrapper {
			adminOpts := client.NewAdminOptions()
			if bootstrapBlocksBatchSize > 0 {
				adminOpts = adminOpts.SetFetchSeriesBlocksBatchSize(bootstrapBlocksBatchSize)
			}
			if bootstrapBlocksConcurrency > 0 {
				adminOpts = adminOpts.SetFetchSeriesBlocksBatchConcurrency(bootstrapBlocksConcurrency)
			}
			adminClient = newMultiAddrAdminClient(
				t, adminOpts, instrumentOpts, setup.shardSet, replicas, instance)
		}

		peersOpts := peers.NewOptions().
			SetResultOptions(bsOpts).
			SetAdminClient(adminClient)

		peersBootstrapper := peers.NewPeersBootstrapper(peersOpts, noOpAll)

		fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
		filePathPrefix := fsOpts.FilePathPrefix()

		bfsOpts := bfs.NewOptions().
			SetResultOptions(bsOpts).
			SetFilesystemOptions(fsOpts)

		var fsBootstrapper bootstrap.Bootstrapper
		if usingPeersBoostrapper {
			fsBootstrapper = bfs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, peersBootstrapper)
		} else {
			fsBootstrapper = bfs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, noOpAll)
		}

		setup.storageOpts = setup.storageOpts.
			SetBootstrapProcess(bootstrap.NewProcess(fsBootstrapper, bsOpts))

		logger := setup.storageOpts.InstrumentOptions().Logger()
		logger = logger.WithFields(xlog.NewLogField("instance", instance))
		iopts := setup.storageOpts.InstrumentOptions().SetLogger(logger)
		if testStatsReporter != nil {
			scope, _ := tally.NewRootScope("", nil, testStatsReporter, 100*time.Millisecond, tally.DefaultSeparator)
			iopts = iopts.SetMetricsScope(scope)
		}
		setup.storageOpts = setup.storageOpts.SetInstrumentOptions(iopts)

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

type testData struct {
	ids       []string
	numPoints int
	start     time.Time
}

func writeTestDataToDisk(
	t *testing.T,
	namespace ts.ID,
	setup *testSetup,
	seriesMaps map[time.Time]seriesList,
) error {
	storageOpts := setup.storageOpts
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	writerBufferSize := fsOpts.WriterBufferSize()
	blockSize := storageOpts.RetentionOptions().BlockSize()
	retentionPeriod := storageOpts.RetentionOptions().RetentionPeriod()
	filePathPrefix := fsOpts.FilePathPrefix()
	newFileMode := fsOpts.NewFileMode()
	newDirectoryMode := fsOpts.NewDirectoryMode()
	writer := fs.NewWriter(blockSize, filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
	encoder := storageOpts.EncoderPool().Get()

	currStart := setup.getNowFn().Truncate(blockSize)
	retentionStart := currStart.Add(-retentionPeriod)
	isValidStart := func(start time.Time) bool {
		return start.Equal(retentionStart) || start.After(retentionStart)
	}

	starts := make(map[time.Time]struct{})
	for start := currStart; isValidStart(start); start = start.Add(-blockSize) {
		starts[start] = struct{}{}
	}

	for start, data := range seriesMaps {
		err := writeToDisk(t, writer, setup.shardSet, encoder, start, namespace, data)
		if err != nil {
			return err
		}
		delete(starts, start)
	}

	// Write remaining files even for empty start periods to avoid unfulfilled ranges
	for start := range starts {
		err := writeToDisk(t, writer, setup.shardSet, encoder, start, namespace, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeToDisk(
	t *testing.T,
	writer fs.FileSetWriter,
	shardSet sharding.ShardSet,
	encoder encoding.Encoder,
	start time.Time,
	namespace ts.ID,
	seriesList seriesList,
) error {
	seriesPerShard := make(map[uint32][]series)
	for _, shard := range shardSet.AllIDs() {
		// Ensure we write out block files for each shard even if there's no data
		seriesPerShard[shard] = make([]series, 0)
	}
	for _, s := range seriesList {
		shard := shardSet.Lookup(s.ID)
		seriesPerShard[shard] = append(seriesPerShard[shard], s)
	}
	data := make([]checked.Bytes, 2)
	for shard, seriesList := range seriesPerShard {
		if err := writer.Open(namespace, shard, start); err != nil {
			return err
		}
		for _, series := range seriesList {
			encoder.Reset(start, 0)
			for _, dp := range series.Data {
				if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
					return err
				}
			}
			stream := encoder.Stream()
			require.NotNil(t, stream)
			segment, err := stream.Segment()
			if err != nil {
				return err
			}
			data[0] = segment.Head
			data[1] = segment.Tail
			checksum := digest.SegmentChecksum(segment)
			if err := writer.WriteAll(series.ID, data, checksum); err != nil {
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
	}

	return nil
}

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

func newClusterShardsRange(from, to uint32, s shard.State) shard.Shards {
	return newClusterShards(newShardsRange(from, to), s)
}

func newClusterEmptyShardsRange() shard.Shards {
	return newClusterShards(nil, shard.Available)
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
