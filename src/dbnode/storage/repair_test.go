// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/repair"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestDatabaseRepairerStartStop(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions().SetRepairOptions(testRepairOptions(ctrl))
	db := NewMockdatabase(ctrl)
	db.EXPECT().Options().Return(opts).AnyTimes()

	databaseRepairer, err := newDatabaseRepairer(db, opts)
	require.NoError(t, err)
	repairer := databaseRepairer.(*dbRepairer)

	var (
		repaired bool
		lock     sync.RWMutex
	)

	repairer.repairFn = func() error {
		lock.Lock()
		repaired = true
		lock.Unlock()
		return nil
	}

	repairer.Start()

	for {
		// Wait for repair to be called
		lock.RLock()
		done := repaired
		lock.RUnlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	repairer.Stop()
	for {
		// Wait for the repairer to stop
		repairer.closedLock.Lock()
		closed := repairer.closed
		repairer.closedLock.Unlock()
		if closed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDatabaseRepairerRepairNotBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions().SetRepairOptions(testRepairOptions(ctrl))
	mockDatabase := NewMockdatabase(ctrl)

	databaseRepairer, err := newDatabaseRepairer(mockDatabase, opts)
	require.NoError(t, err)
	repairer := databaseRepairer.(*dbRepairer)

	mockDatabase.EXPECT().IsBootstrapped().Return(false)
	require.Nil(t, repairer.Repair())
}

func TestDatabaseShardRepairerRepair(t *testing.T) {
	testDatabaseShardRepairerRepair(t, false)
}

func TestDatabaseShardRepairerRepairWithLimit(t *testing.T) {
	testDatabaseShardRepairerRepair(t, true)
}

func testDatabaseShardRepairerRepair(t *testing.T, withLimit bool) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockAdminSession(ctrl)
	session.EXPECT().Origin().Return(topology.NewHost("0", "addr0")).AnyTimes()
	session.EXPECT().TopologyMap().AnyTimes()

	mockClient := client.NewMockAdminClient(ctrl)
	mockClient.EXPECT().DefaultAdminSession().Return(session, nil).AnyTimes()

	var (
		rpOpts = testRepairOptions(ctrl).
			SetAdminClients([]client.AdminClient{mockClient})
		now            = xtime.Now()
		nowFn          = func() time.Time { return now.ToTime() }
		opts           = DefaultTestOptions()
		copts          = opts.ClockOptions()
		iopts          = opts.InstrumentOptions()
		rtopts         = defaultTestRetentionOpts
		memTrackerOpts = NewMemoryTrackerOptions(1)
		memTracker     = NewMemoryTracker(memTrackerOpts)
	)
	if withLimit {
		opts = opts.SetMemoryTracker(memTracker)
	}

	opts = opts.
		SetClockOptions(copts.SetNowFn(nowFn)).
		SetInstrumentOptions(iopts.SetMetricsScope(tally.NoopScope))

	var (
		namespaceID     = ident.StringID("testNamespace")
		start           = now
		end             = now.Add(rtopts.BlockSize())
		repairTimeRange = xtime.Range{Start: start, End: end}
		fetchOpts       = block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  false,
		}

		sizes     = []int64{1, 2, 3, 4}
		checksums = []uint32{4, 5, 6, 7}
		lastRead  = now.Add(-time.Minute)
		shardID   = uint32(0)
		shard     = NewMockdatabaseShard(ctrl)

		numIters = 1
	)

	if withLimit {
		numIters = 2
		shard.EXPECT().LoadBlocks(gomock.Any()).Return(nil)
		shard.EXPECT().LoadBlocks(gomock.Any()).DoAndReturn(func(*result.Map) error {
			// Return an error that we've hit the limit, but also start a delayed
			// goroutine to release the throttle repair process.
			go func() {
				time.Sleep(10 * time.Millisecond)
				memTracker.DecPendingLoadedBytes()
			}()
			return ErrDatabaseLoadLimitHit
		})
		shard.EXPECT().LoadBlocks(gomock.Any()).Return(nil)
	} else {
		shard.EXPECT().LoadBlocks(gomock.Any()).Return(nil)
	}

	for i := 0; i < numIters; i++ {
		expectedResults := block.NewFetchBlocksMetadataResults()
		results := block.NewFetchBlockMetadataResults()
		results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute),
			sizes[0], &checksums[0], lastRead, nil))
		results.Add(block.NewFetchBlockMetadataResult(now.Add(time.Hour),
			sizes[1], &checksums[1], lastRead, nil))
		expectedResults.Add(block.NewFetchBlocksMetadataResult(ident.StringID("foo"), nil, results))
		results = block.NewFetchBlockMetadataResults()
		results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute),
			sizes[2], &checksums[2], lastRead, nil))
		expectedResults.Add(block.NewFetchBlocksMetadataResult(ident.StringID("bar"), nil, results))

		var (
			any             = gomock.Any()
			nonNilPageToken = PageToken("non-nil-page-token")
		)
		// Ensure that the Repair logic will call FetchBlocksMetadataV2 in a loop until
		// it receives a nil page token.
		shard.EXPECT().
			FetchBlocksMetadataV2(any, start, end, any, nil, fetchOpts).
			Return(nil, nonNilPageToken, nil)
		shard.EXPECT().
			FetchBlocksMetadataV2(any, start, end, any, nonNilPageToken, fetchOpts).
			Return(expectedResults, nil, nil)
		shard.EXPECT().ID().Return(shardID).AnyTimes()

		peerIter := client.NewMockPeerBlockMetadataIter(ctrl)
		inputBlocks := []block.ReplicaMetadata{
			{
				Host:     topology.NewHost("1", "addr1"),
				Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(30*time.Minute), sizes[0], &checksums[0], lastRead),
			},
			{
				Host:     topology.NewHost("1", "addr1"),
				Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(time.Hour), sizes[0], &checksums[1], lastRead),
			},
			{
				Host: topology.NewHost("1", "addr1"),
				// Mismatch checksum so should trigger repair of this series.
				Metadata: block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now.Add(30*time.Minute), sizes[2], &checksums[3], lastRead),
			},
		}

		gomock.InOrder(
			peerIter.EXPECT().Next().Return(true),
			peerIter.EXPECT().Current().Return(inputBlocks[0].Host, inputBlocks[0].Metadata),
			peerIter.EXPECT().Next().Return(true),
			peerIter.EXPECT().Current().Return(inputBlocks[1].Host, inputBlocks[1].Metadata),
			peerIter.EXPECT().Next().Return(true),
			peerIter.EXPECT().Current().Return(inputBlocks[2].Host, inputBlocks[2].Metadata),
			peerIter.EXPECT().Next().Return(false),
			peerIter.EXPECT().Err().Return(nil),
		)
		session.EXPECT().
			FetchBlocksMetadataFromPeers(namespaceID, shardID, start, end,
				rpOpts.RepairConsistencyLevel(), gomock.Any()).
			Return(peerIter, nil)

		peerBlocksIter := client.NewMockPeerBlocksIter(ctrl)
		dbBlock1 := block.NewMockDatabaseBlock(ctrl)
		dbBlock1.EXPECT().StartTime().Return(inputBlocks[2].Metadata.Start).AnyTimes()
		dbBlock2 := block.NewMockDatabaseBlock(ctrl)
		dbBlock2.EXPECT().StartTime().Return(inputBlocks[2].Metadata.Start).AnyTimes()
		// Ensure merging logic works.
		dbBlock1.EXPECT().Merge(dbBlock2)
		gomock.InOrder(
			peerBlocksIter.EXPECT().Next().Return(true),
			peerBlocksIter.EXPECT().Current().Return(inputBlocks[2].Host, inputBlocks[2].Metadata.ID, dbBlock1),
			peerBlocksIter.EXPECT().Next().Return(true),
			peerBlocksIter.EXPECT().Current().Return(inputBlocks[2].Host, inputBlocks[2].Metadata.ID, dbBlock2),
			peerBlocksIter.EXPECT().Next().Return(false),
		)
		nsMeta, err := namespace.NewMetadata(namespaceID, namespace.NewOptions())
		require.NoError(t, err)
		session.EXPECT().
			FetchBlocksFromPeers(nsMeta, shardID, rpOpts.RepairConsistencyLevel(), inputBlocks[2:], gomock.Any()).
			Return(peerBlocksIter, nil)

		var (
			resNamespace ident.ID
			resShard     databaseShard
			resDiff      repair.MetadataComparisonResult
		)

		databaseShardRepairer := newShardRepairer(opts, rpOpts)
		repairer := databaseShardRepairer.(shardRepairer)
		repairer.recordFn = func(origin topology.Host, nsID ident.ID, shard databaseShard,
			diffRes repair.MetadataComparisonResult) {
			resNamespace = nsID
			resShard = shard
			resDiff = diffRes
		}

		var (
			ctx   = context.NewBackground()
			nsCtx = namespace.Context{ID: namespaceID}
		)
		require.NoError(t, err)
		repairer.Repair(ctx, nsCtx, nsMeta, repairTimeRange, shard)

		require.Equal(t, namespaceID, resNamespace)
		require.Equal(t, resShard, shard)
		require.Equal(t, int64(2), resDiff.NumSeries)
		require.Equal(t, int64(3), resDiff.NumBlocks)

		checksumDiffSeries := resDiff.ChecksumDifferences.Series()
		require.Equal(t, 1, checksumDiffSeries.Len())
		series, exists := checksumDiffSeries.Get(ident.StringID("bar"))
		require.True(t, exists)
		blocks := series.Metadata.Blocks()
		require.Equal(t, 1, len(blocks))
		currBlock, exists := blocks[now.Add(30*time.Minute)]
		require.True(t, exists)
		require.Equal(t, now.Add(30*time.Minute), currBlock.Start())
		expected := []block.ReplicaMetadata{
			// Checksum difference for series "bar".
			{Host: topology.NewHost("0", "addr0"), Metadata: block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now.Add(30*time.Minute), sizes[2], &checksums[2], lastRead)},
			{Host: topology.NewHost("1", "addr1"), Metadata: inputBlocks[2].Metadata},
		}
		require.Equal(t, expected, currBlock.Metadata())

		sizeDiffSeries := resDiff.SizeDifferences.Series()
		require.Equal(t, 1, sizeDiffSeries.Len())
		series, exists = sizeDiffSeries.Get(ident.StringID("foo"))
		require.True(t, exists)
		blocks = series.Metadata.Blocks()
		require.Equal(t, 1, len(blocks))
		currBlock, exists = blocks[now.Add(time.Hour)]
		require.True(t, exists)
		require.Equal(t, now.Add(time.Hour), currBlock.Start())
		expected = []block.ReplicaMetadata{
			// Size difference for series "foo".
			{Host: topology.NewHost("0", "addr0"), Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(time.Hour), sizes[1], &checksums[1], lastRead)},
			{Host: topology.NewHost("1", "addr1"), Metadata: inputBlocks[1].Metadata},
		}
		require.Equal(t, expected, currBlock.Metadata())
	}
}

type multiSessionTestMock struct {
	host    topology.Host
	client  *client.MockAdminClient
	session *client.MockAdminSession
	topoMap *topology.MockMap
}

func TestDatabaseShardRepairerRepairMultiSession(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Origin is always zero (on both clients) and hosts[0] and hosts[1]
	// represents other nodes in different clusters.
	origin := topology.NewHost("0", "addr0")
	mocks := []multiSessionTestMock{
		{
			host:    topology.NewHost("1", "addr1"),
			client:  client.NewMockAdminClient(ctrl),
			session: client.NewMockAdminSession(ctrl),
			topoMap: topology.NewMockMap(ctrl),
		},
		{
			host:    topology.NewHost("2", "addr2"),
			client:  client.NewMockAdminClient(ctrl),
			session: client.NewMockAdminSession(ctrl),
			topoMap: topology.NewMockMap(ctrl),
		},
	}

	var mockClients []client.AdminClient
	var hosts []topology.Host
	for _, mock := range mocks {
		mock.session.EXPECT().Origin().Return(origin).AnyTimes()
		mock.client.EXPECT().DefaultAdminSession().Return(mock.session, nil)
		mock.session.EXPECT().TopologyMap().Return(mock.topoMap, nil)
		mockClients = append(mockClients, mock.client)
		hosts = append(hosts, mock.host)
	}

	var (
		rpOpts = testRepairOptions(ctrl).
			SetAdminClients(mockClients)
		now    = xtime.Now()
		nowFn  = func() time.Time { return now.ToTime() }
		opts   = DefaultTestOptions()
		copts  = opts.ClockOptions()
		iopts  = opts.InstrumentOptions()
		rtopts = defaultTestRetentionOpts
		scope  = tally.NewTestScope("", nil)
	)

	opts = opts.
		SetClockOptions(copts.SetNowFn(nowFn)).
		SetInstrumentOptions(iopts.SetMetricsScope(scope))

	var (
		namespaceID     = ident.StringID("testNamespace")
		start           = now
		end             = now.Add(rtopts.BlockSize())
		repairTimeRange = xtime.Range{Start: start, End: end}
		fetchOpts       = block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  false,
		}

		sizes     = []int64{3423, 987, 8463, 578}
		checksums = []uint32{4, 5, 6, 7}
		lastRead  = now.Add(-time.Minute)
		shardID   = uint32(0)
		shard     = NewMockdatabaseShard(ctrl)
	)

	expectedResults := block.NewFetchBlocksMetadataResults()
	results := block.NewFetchBlockMetadataResults()
	results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute),
		sizes[0], &checksums[0], lastRead, nil))
	results.Add(block.NewFetchBlockMetadataResult(now.Add(time.Hour),
		sizes[1], &checksums[1], lastRead, nil))
	expectedResults.Add(block.NewFetchBlocksMetadataResult(ident.StringID("foo"), nil, results))
	results = block.NewFetchBlockMetadataResults()
	results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute),
		sizes[2], &checksums[2], lastRead, nil))
	expectedResults.Add(block.NewFetchBlocksMetadataResult(ident.StringID("bar"), nil, results))

	var (
		any             = gomock.Any()
		nonNilPageToken = PageToken("non-nil-page-token")
	)
	// Ensure that the Repair logic will call FetchBlocksMetadataV2 in a loop until
	// it receives a nil page token.
	shard.EXPECT().
		FetchBlocksMetadataV2(any, start, end, any, nil, fetchOpts).
		Return(nil, nonNilPageToken, nil)
	shard.EXPECT().
		FetchBlocksMetadataV2(any, start, end, any, nonNilPageToken, fetchOpts).
		Return(expectedResults, nil, nil)
	shard.EXPECT().ID().Return(shardID).AnyTimes()
	shard.EXPECT().LoadBlocks(gomock.Any()).Return(nil)

	inputBlocks := []block.ReplicaMetadata{
		{
			// Peer block size size[2] is different from origin block size size[0]
			Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(30*time.Minute), sizes[2], &checksums[0], lastRead),
		},
		{
			// Peer block size size[3] is different from origin block size size[1]
			Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(time.Hour), sizes[3], &checksums[1], lastRead),
		},
		{
			// Mismatch checksum so should trigger repair of this series.
			Metadata: block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now.Add(30*time.Minute), sizes[2], &checksums[3], lastRead),
		},
	}

	for i, mock := range mocks {
		mockTopoMap := mock.topoMap
		for _, host := range hosts {
			iClosure := i
			mockTopoMap.EXPECT().LookupHostShardSet(host.ID()).DoAndReturn(func(id string) (topology.HostShardSet, bool) {
				if iClosure == 0 && id == hosts[0].ID() {
					return nil, true
				}
				if iClosure == 1 && id == hosts[1].ID() {
					return nil, true
				}
				return nil, false
			}).AnyTimes()
		}
	}

	nsMeta, err := namespace.NewMetadata(namespaceID, namespace.NewOptions())
	for i, mock := range mocks {
		session := mock.session
		// Make a copy of the input blocks where the host is set to the host for
		// the cluster associated with the current session.
		inputBlocksForSession := make([]block.ReplicaMetadata, len(inputBlocks))
		copy(inputBlocksForSession, inputBlocks)
		for j := range inputBlocksForSession {
			inputBlocksForSession[j].Host = hosts[i]
		}

		peerIter := client.NewMockPeerBlockMetadataIter(ctrl)
		gomock.InOrder(
			peerIter.EXPECT().Next().Return(true),
			peerIter.EXPECT().Current().Return(inputBlocksForSession[0].Host, inputBlocks[0].Metadata),
			peerIter.EXPECT().Next().Return(true),
			peerIter.EXPECT().Current().Return(inputBlocksForSession[1].Host, inputBlocks[1].Metadata),
			peerIter.EXPECT().Next().Return(true),
			peerIter.EXPECT().Current().Return(inputBlocksForSession[2].Host, inputBlocks[2].Metadata),
			peerIter.EXPECT().Next().Return(false),
			peerIter.EXPECT().Err().Return(nil),
		)
		session.EXPECT().
			FetchBlocksMetadataFromPeers(namespaceID, shardID, start, end,
				rpOpts.RepairConsistencyLevel(), gomock.Any()).
			Return(peerIter, nil)

		peerBlocksIter := client.NewMockPeerBlocksIter(ctrl)
		dbBlock1 := block.NewMockDatabaseBlock(ctrl)
		dbBlock1.EXPECT().StartTime().Return(inputBlocksForSession[2].Metadata.Start).AnyTimes()
		dbBlock2 := block.NewMockDatabaseBlock(ctrl)
		dbBlock2.EXPECT().StartTime().Return(inputBlocksForSession[2].Metadata.Start).AnyTimes()
		// Ensure merging logic works. Nede AnyTimes() because the Merge() will only be called on dbBlock1
		// for the first session (all subsequent blocks from other sessions will get merged into dbBlock1
		// from the first session.)
		dbBlock1.EXPECT().Merge(dbBlock2).AnyTimes()
		gomock.InOrder(
			peerBlocksIter.EXPECT().Next().Return(true),
			peerBlocksIter.EXPECT().Current().Return(inputBlocksForSession[2].Host, inputBlocks[2].Metadata.ID, dbBlock1),
			peerBlocksIter.EXPECT().Next().Return(true),
			peerBlocksIter.EXPECT().Current().Return(inputBlocksForSession[2].Host, inputBlocks[2].Metadata.ID, dbBlock2),
			peerBlocksIter.EXPECT().Next().Return(false),
		)
		require.NoError(t, err)
		session.EXPECT().
			FetchBlocksFromPeers(nsMeta, shardID, rpOpts.RepairConsistencyLevel(), inputBlocksForSession[2:], gomock.Any()).
			Return(peerBlocksIter, nil)
	}

	databaseShardRepairer := newShardRepairer(opts, rpOpts)
	repairer := databaseShardRepairer.(shardRepairer)

	var (
		ctx   = context.NewBackground()
		nsCtx = namespace.Context{ID: namespaceID}
	)
	resDiff, err := repairer.Repair(ctx, nsCtx, nsMeta, repairTimeRange, shard)

	require.NoError(t, err)
	require.Equal(t, int64(2), resDiff.NumSeries)
	require.Equal(t, int64(3), resDiff.NumBlocks)

	checksumDiffSeries := resDiff.ChecksumDifferences.Series()
	require.Equal(t, 1, checksumDiffSeries.Len())
	series, exists := checksumDiffSeries.Get(ident.StringID("bar"))
	require.True(t, exists)
	blocks := series.Metadata.Blocks()
	require.Equal(t, 1, len(blocks))
	currBlock, exists := blocks[now.Add(30*time.Minute)]
	require.True(t, exists)
	require.Equal(t, now.Add(30*time.Minute), currBlock.Start())
	expected := []block.ReplicaMetadata{
		// Checksum difference for series "bar".
		{Host: origin, Metadata: block.NewMetadata(ident.StringID("bar"), ident.Tags{}, now.Add(30*time.Minute), sizes[2], &checksums[2], lastRead)},
		{Host: hosts[0], Metadata: inputBlocks[2].Metadata},
		{Host: hosts[1], Metadata: inputBlocks[2].Metadata},
	}
	require.Equal(t, expected, currBlock.Metadata())

	sizeDiffSeries := resDiff.SizeDifferences.Series()
	require.Equal(t, 1, sizeDiffSeries.Len())
	series, exists = sizeDiffSeries.Get(ident.StringID("foo"))
	require.True(t, exists)
	blocks = series.Metadata.Blocks()
	require.Equal(t, 2, len(blocks))
	// Validate first block
	currBlock, exists = blocks[now.Add(30*time.Minute)]
	require.True(t, exists)
	require.Equal(t, now.Add(30*time.Minute), currBlock.Start())
	expected = []block.ReplicaMetadata{
		// Size difference for series "foo".
		{Host: origin, Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(30*time.Minute), sizes[0], &checksums[0], lastRead)},
		{Host: hosts[0], Metadata: inputBlocks[0].Metadata},
		{Host: hosts[1], Metadata: inputBlocks[0].Metadata},
	}
	require.Equal(t, expected, currBlock.Metadata())
	// Validate second block
	currBlock, exists = blocks[now.Add(time.Hour)]
	require.True(t, exists)
	require.Equal(t, now.Add(time.Hour), currBlock.Start())
	expected = []block.ReplicaMetadata{
		// Size difference for series "foo".
		{Host: origin, Metadata: block.NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(time.Hour), sizes[1], &checksums[1], lastRead)},
		{Host: hosts[0], Metadata: inputBlocks[1].Metadata},
		{Host: hosts[1], Metadata: inputBlocks[1].Metadata},
	}
	require.Equal(t, expected, currBlock.Metadata())

	// Validate the expected metrics were emitted
	scopeSnapshot := scope.Snapshot()
	countersSnapshot := scopeSnapshot.Counters()
	gaugesSnapshot := scopeSnapshot.Gauges()
	require.Equal(t, int64(2),
		countersSnapshot["repair.series+namespace=testNamespace,resultType=total,shard=0"].Value())
	require.Equal(t, int64(3),
		countersSnapshot["repair.blocks+namespace=testNamespace,resultType=total,shard=0"].Value())
	// Validate that first block's divergence is emitted instead of second block because first block is diverged
	// more than second block from its peers.
	scopeTags := map[string]string{"namespace": "testNamespace", "resultType": "sizeDiff", "shard": "0"}
	require.Equal(t, float64(sizes[0]-sizes[2]),
		gaugesSnapshot[tally.KeyForPrefixedStringMap("repair.max-block-size-diff", scopeTags)].Value())
	require.Equal(t, float64(100*(sizes[0]-sizes[2]))/float64(sizes[0]),
		gaugesSnapshot[tally.KeyForPrefixedStringMap("repair.max-block-size-diff-as-percentage", scopeTags)].Value())
}

type expectedRepair struct {
	expectedRepairRange xtime.Range
	mockRepairResult    error
}

func TestDatabaseRepairPrioritizationLogic(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		rOpts = retention.NewOptions().
			SetRetentionPeriod(retention.NewOptions().BlockSize() * 2)
		nsOpts = namespace.NewOptions().
			SetRetentionOptions(rOpts)
		blockSize = rOpts.BlockSize()

		// Set current time such that the previous block is flushable.
		now = xtime.Now().Truncate(blockSize).Add(rOpts.BufferPast()).Add(time.Second)

		flushTimeStart = retention.FlushTimeStart(rOpts, now)
		flushTimeEnd   = retention.FlushTimeEnd(rOpts, now)
	)
	require.NoError(t, nsOpts.Validate())
	// Ensure only two flushable blocks in retention to make test logic simpler.
	require.Equal(t, blockSize, flushTimeEnd.Sub(flushTimeStart))

	testCases := []struct {
		title             string
		repairState       repairStatesByNs
		expectedNS1Repair expectedRepair
		expectedNS2Repair expectedRepair
	}{
		{
			title: "repairs most recent block if no repair state",
			expectedNS1Repair: expectedRepair{
				expectedRepairRange: xtime.Range{Start: flushTimeEnd, End: flushTimeEnd.Add(blockSize)},
			},
			expectedNS2Repair: expectedRepair{
				expectedRepairRange: xtime.Range{Start: flushTimeEnd, End: flushTimeEnd.Add(blockSize)},
			},
		},
		{
			title: "repairs next unrepaired block in reverse order if some (but not all) blocks have been repaired",
			repairState: repairStatesByNs{
				"ns1": namespaceRepairStateByTime{
					flushTimeEnd: repairState{
						Status:      repairSuccess,
						LastAttempt: 0,
					},
				},
				"ns2": namespaceRepairStateByTime{
					flushTimeEnd: repairState{
						Status:      repairSuccess,
						LastAttempt: 0,
					},
				},
			},
			expectedNS1Repair: expectedRepair{
				expectedRepairRange: xtime.Range{Start: flushTimeStart, End: flushTimeStart.Add(blockSize)},
			},
			expectedNS2Repair: expectedRepair{
				expectedRepairRange: xtime.Range{Start: flushTimeStart, End: flushTimeStart.Add(blockSize)},
			},
		},
		{
			title: "repairs least recently repaired block if all blocks have been repaired",
			repairState: repairStatesByNs{
				"ns1": namespaceRepairStateByTime{
					flushTimeStart: repairState{
						Status:      repairSuccess,
						LastAttempt: 0,
					},
					flushTimeEnd: repairState{
						Status:      repairSuccess,
						LastAttempt: xtime.UnixNano(time.Second),
					},
				},
				"ns2": namespaceRepairStateByTime{
					flushTimeStart: repairState{
						Status:      repairSuccess,
						LastAttempt: 0,
					},
					flushTimeEnd: repairState{
						Status:      repairSuccess,
						LastAttempt: xtime.UnixNano(time.Second),
					},
				},
			},
			expectedNS1Repair: expectedRepair{
				expectedRepairRange: xtime.Range{Start: flushTimeStart, End: flushTimeStart.Add(blockSize)},
			},
			expectedNS2Repair: expectedRepair{
				expectedRepairRange: xtime.Range{Start: flushTimeStart, End: flushTimeStart.Add(blockSize)},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			opts := DefaultTestOptions().SetRepairOptions(testRepairOptions(ctrl))
			mockDatabase := NewMockdatabase(ctrl)

			databaseRepairer, err := newDatabaseRepairer(mockDatabase, opts)
			require.NoError(t, err)
			repairer := databaseRepairer.(*dbRepairer)
			repairer.nowFn = func() time.Time {
				return now.ToTime()
			}
			if tc.repairState == nil {
				tc.repairState = repairStatesByNs{}
			}
			repairer.repairStatesByNs = tc.repairState

			mockDatabase.EXPECT().IsBootstrapped().Return(true)

			var (
				ns1        = NewMockdatabaseNamespace(ctrl)
				ns2        = NewMockdatabaseNamespace(ctrl)
				namespaces = []databaseNamespace{ns1, ns2}
			)
			ns1.EXPECT().Options().Return(nsOpts).AnyTimes()
			ns2.EXPECT().Options().Return(nsOpts).AnyTimes()

			ns1.EXPECT().ID().Return(ident.StringID("ns1")).AnyTimes()
			ns2.EXPECT().ID().Return(ident.StringID("ns2")).AnyTimes()

			ns1.EXPECT().Repair(gomock.Any(), tc.expectedNS1Repair.expectedRepairRange, NamespaceRepairOptions{})
			ns2.EXPECT().Repair(gomock.Any(), tc.expectedNS2Repair.expectedRepairRange, NamespaceRepairOptions{})

			mockDatabase.EXPECT().OwnedNamespaces().Return(namespaces, nil)
			require.Nil(t, repairer.Repair())
		})
	}
}

// Database repairer repairs blocks in decreasing time ranges for each namespace. If database repairer fails to
// repair a time range of a namespace then instead of skipping repair of all past time ranges of that namespace, test
// that database repaier tries to repair the past corrupt time range of that namespace.
func TestDatabaseRepairSkipsPoisonShard(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		rOpts = retention.NewOptions().
			SetRetentionPeriod(retention.NewOptions().BlockSize() * 2)
		nsOpts = namespace.NewOptions().
			SetRetentionOptions(rOpts)
		blockSize = rOpts.BlockSize()

		// Set current time such that the previous block is flushable.
		now = xtime.Now().Truncate(blockSize).Add(rOpts.BufferPast()).Add(time.Second)

		flushTimeStart = retention.FlushTimeStart(rOpts, now)
		flushTimeEnd   = retention.FlushTimeEnd(rOpts, now)
	)
	require.NoError(t, nsOpts.Validate())
	// Ensure only two flushable blocks in retention to make test logic simpler.
	require.Equal(t, blockSize, flushTimeEnd.Sub(flushTimeStart))

	testCases := []struct {
		title              string
		repairState        repairStatesByNs
		expectedNS1Repairs []expectedRepair
		expectedNS2Repairs []expectedRepair
	}{
		{
			// Test that corrupt ns1 time range (flushTimeEnd, flushTimeEnd + blockSize) does not prevent past time
			// ranges (flushTimeStart, flushTimeStart + blockSize) from being repaired. Also test that least recently
			// repaired policy is honored even when repairing one of the time ranges (flushTimeStart, flushTimeStart +
			// blockSize) on ns2 fails.
			title: "attempts to keep repairing time ranges before poison time ranges",
			repairState: repairStatesByNs{
				"ns2": namespaceRepairStateByTime{
					flushTimeEnd: repairState{
						Status:      repairSuccess,
						LastAttempt: 0,
					},
				},
			},
			expectedNS1Repairs: []expectedRepair{
				{
					xtime.Range{Start: flushTimeEnd, End: flushTimeEnd.Add(blockSize)},
					errors.New("ns1 repair error"),
				},
				{
					xtime.Range{Start: flushTimeStart, End: flushTimeStart.Add(blockSize)},
					nil,
				},
			},
			expectedNS2Repairs: []expectedRepair{
				{
					xtime.Range{Start: flushTimeStart, End: flushTimeStart.Add(blockSize)},
					errors.New("ns2 repair error"),
				},
				{
					xtime.Range{Start: flushTimeEnd, End: flushTimeEnd.Add(blockSize)},
					nil,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			opts := DefaultTestOptions().SetRepairOptions(testRepairOptions(ctrl))
			mockDatabase := NewMockdatabase(ctrl)

			databaseRepairer, err := newDatabaseRepairer(mockDatabase, opts)
			require.NoError(t, err)
			repairer := databaseRepairer.(*dbRepairer)
			repairer.nowFn = func() time.Time {
				return now.ToTime()
			}
			if tc.repairState == nil {
				tc.repairState = repairStatesByNs{}
			}
			repairer.repairStatesByNs = tc.repairState

			mockDatabase.EXPECT().IsBootstrapped().Return(true)

			var (
				ns1        = NewMockdatabaseNamespace(ctrl)
				ns2        = NewMockdatabaseNamespace(ctrl)
				namespaces = []databaseNamespace{ns1, ns2}
			)
			ns1.EXPECT().Options().Return(nsOpts).AnyTimes()
			ns2.EXPECT().Options().Return(nsOpts).AnyTimes()

			ns1.EXPECT().ID().Return(ident.StringID("ns1")).AnyTimes()
			ns2.EXPECT().ID().Return(ident.StringID("ns2")).AnyTimes()

			//Setup expected ns1 repair invocations for each repaired time range
			var ns1RepairExpectations = make([]*gomock.Call, len(tc.expectedNS1Repairs))
			for i, ns1Repair := range tc.expectedNS1Repairs {
				ns1RepairExpectations[i] = ns1.EXPECT().
					Repair(gomock.Any(), ns1Repair.expectedRepairRange, NamespaceRepairOptions{}).
					Return(ns1Repair.mockRepairResult)
			}
			gomock.InOrder(ns1RepairExpectations...)

			//Setup expected ns2 repair invocations for each repaired time range
			var ns2RepairExpectations = make([]*gomock.Call, len(tc.expectedNS2Repairs))
			for i, ns2Repair := range tc.expectedNS2Repairs {
				ns2RepairExpectations[i] = ns2.EXPECT().
					Repair(gomock.Any(), ns2Repair.expectedRepairRange, NamespaceRepairOptions{}).
					Return(ns2Repair.mockRepairResult)
			}
			gomock.InOrder(ns2RepairExpectations...)

			mockDatabase.EXPECT().OwnedNamespaces().Return(namespaces, nil)

			require.NotNil(t, repairer.Repair())
		})
	}
}
