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

package storage

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testRepairOptions(ctrl *gomock.Controller) repair.Options {
	return repair.NewOptions().
		SetAdminClient(client.NewMockAdminClient(ctrl)).
		SetRepairInterval(time.Second).
		SetRepairTimeOffset(500 * time.Millisecond).
		SetRepairTimeJitter(300 * time.Millisecond).
		SetRepairCheckInterval(100 * time.Millisecond)
}

func TestDatabaseRepairerStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDatabase := newMockDatabase()
	mockDatabase.opts = mockDatabase.opts.SetRepairOptions(testRepairOptions(ctrl))

	databaseRepairer, err := newDatabaseRepairer(mockDatabase)
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
		time.Sleep(time.Second)
	}

	repairer.Stop()
	for {
		// Wait for the repairer to stop
		repairer.Lock()
		closed := repairer.closed
		repairer.Unlock()
		if closed {
			break
		}
		time.Sleep(time.Second)
	}
}

func TestDatabaseRepairerHaveNotReachedOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		repairInterval   = 2 * time.Hour
		repairTimeOffset = time.Hour
		now              = time.Now().Truncate(repairInterval).Add(30 * time.Minute)
		repaired         = false
		numIter          = 0
	)

	nowFn := func() time.Time { return now }
	mockDatabase := newMockDatabase()
	clockOpts := mockDatabase.opts.ClockOptions().SetNowFn(nowFn)
	repairOpts := testRepairOptions(ctrl).
		SetRepairInterval(repairInterval).
		SetRepairTimeOffset(repairTimeOffset)
	mockDatabase.opts = mockDatabase.opts.
		SetClockOptions(clockOpts.SetNowFn(nowFn)).
		SetRepairOptions(repairOpts)

	databaseRepairer, err := newDatabaseRepairer(mockDatabase)
	require.NoError(t, err)
	repairer := databaseRepairer.(*dbRepairer)

	repairer.repairFn = func() error {
		repaired = true
		return nil
	}

	repairer.sleepFn = func(_ time.Duration) {
		if numIter == 0 {
			repairer.closed = true
		}
		numIter++
	}

	repairer.run()
	require.Equal(t, 1, numIter)
	require.False(t, repaired)
}

func TestDatabaseRepairerOnlyOncePerInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		repairInterval   = 2 * time.Hour
		repairTimeOffset = time.Hour
		now              = time.Now().Truncate(repairInterval).Add(90 * time.Minute)
		numRepairs       = 0
		numIter          = 0
	)

	nowFn := func() time.Time {
		switch numIter {
		case 0:
			return now
		case 1:
			return now.Add(time.Minute)
		case 2:
			return now.Add(time.Hour)
		default:
			return now.Add(2 * time.Hour)
		}
	}

	mockDatabase := newMockDatabase()
	clockOpts := mockDatabase.opts.ClockOptions().SetNowFn(nowFn)
	repairOpts := testRepairOptions(ctrl).
		SetRepairInterval(repairInterval).
		SetRepairTimeOffset(repairTimeOffset)
	mockDatabase.opts = mockDatabase.opts.
		SetClockOptions(clockOpts.SetNowFn(nowFn)).
		SetRepairOptions(repairOpts)

	databaseRepairer, err := newDatabaseRepairer(mockDatabase)
	require.NoError(t, err)
	repairer := databaseRepairer.(*dbRepairer)

	repairer.repairFn = func() error {
		numRepairs++
		return nil
	}

	repairer.sleepFn = func(_ time.Duration) {
		if numIter == 3 {
			repairer.closed = true
		}
		numIter++
	}

	repairer.run()
	require.Equal(t, 4, numIter)
	require.Equal(t, 2, numRepairs)
}

func TestDatabaseRepairerRepairNotBootstrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDatabase := newMockDatabase()
	mockDatabase.opts = mockDatabase.opts.SetRepairOptions(testRepairOptions(ctrl))

	databaseRepairer, err := newDatabaseRepairer(mockDatabase)
	require.NoError(t, err)
	repairer := databaseRepairer.(*dbRepairer)

	mockDatabase.bs = bootstrapNotStarted
	require.Nil(t, repairer.Repair())
}

func TestDatabaseShardRepairerRecordDifferences(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	session := client.NewMockAdminSession(ctrl)
	session.EXPECT().Origin().Return(topology.NewHost("0", "addr0"))
	session.EXPECT().Replicas().Return(2)

	mockClient := client.NewMockAdminClient(ctrl)
	mockClient.EXPECT().DefaultAdminSession().Return(session, nil)

	rpOpts := testRepairOptions(ctrl).SetAdminClient(mockClient)

	now := time.Now()
	nowFn := func() time.Time { return now }
	opts := testDatabaseOptions()
	copts := opts.ClockOptions()
	iopts := opts.InstrumentOptions()
	rtopts := opts.RetentionOptions()
	opts = opts.
		SetClockOptions(copts.SetNowFn(nowFn)).
		SetInstrumentOptions(iopts.SetMetricsScope(tally.NoopScope))

	var (
		namespace       = ts.StringID("testNamespace")
		start           = now
		end             = now.Add(rtopts.BlockSize())
		repairTimeRange = xtime.Range{Start: start, End: end}
	)

	sizes := []int64{1, 2, 3}
	checksums := []uint32{4, 5, 6}
	shardID := uint32(0)
	shard := NewMockdatabaseShard(ctrl)

	expectedResults := block.NewFetchBlocksMetadataResults()
	results := block.NewFetchBlockMetadataResults()
	results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute), &sizes[0], &checksums[0], nil))
	results.Add(block.NewFetchBlockMetadataResult(now.Add(time.Hour), &sizes[1], &checksums[1], nil))
	expectedResults.Add(block.NewFetchBlocksMetadataResult(ts.StringID("foo"), results))
	results = block.NewFetchBlockMetadataResults()
	results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute), &sizes[2], &checksums[2], nil))
	expectedResults.Add(block.NewFetchBlocksMetadataResult(ts.StringID("bar"), results))
	shard.EXPECT().
		FetchBlocksMetadata(gomock.Any(), start, end, gomock.Any(), int64(0), true, true).
		Return(expectedResults, nil)
	shard.EXPECT().ID().Return(shardID).AnyTimes()

	peerIter := client.NewMockPeerBlocksMetadataIter(ctrl)
	inputBlocks := []struct {
		host topology.Host
		meta block.BlocksMetadata
	}{
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata(ts.StringID("foo"), []block.Metadata{
			block.NewMetadata(now.Add(30*time.Minute), sizes[0], &checksums[0]),
			block.NewMetadata(now.Add(time.Hour), sizes[0], &checksums[1]),
		})},
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata(ts.StringID("bar"), []block.Metadata{
			block.NewMetadata(now.Add(30*time.Minute), sizes[2], &checksums[2]),
		})},
	}

	gomock.InOrder(
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[0].host, inputBlocks[0].meta),
		peerIter.EXPECT().Next().Return(true),
		peerIter.EXPECT().Current().Return(inputBlocks[1].host, inputBlocks[1].meta),
		peerIter.EXPECT().Next().Return(false),
		peerIter.EXPECT().Err().Return(nil),
	)
	session.EXPECT().
		FetchBlocksMetadataFromPeers(namespace, shardID, start, end).
		Return(peerIter, nil)

	var (
		resNamespace ts.ID
		resShard     databaseShard
		resDiff      repair.MetadataComparisonResult
	)

	databaseShardRepairer, err := newShardRepairer(opts, rpOpts)
	require.NoError(t, err)
	repairer := databaseShardRepairer.(shardRepairer)
	repairer.recordFn = func(namespace ts.ID, shard databaseShard, res repair.Result) {
		resNamespace = namespace
		resShard = shard
		resDiff = res.DifferenceSummary
	}
	repairer.repairFn = func(namespace ts.ID, shard databaseShard, diffRes repair.MetadataComparisonResult) (repair.ExecutionMetrics, error) {
		return repair.ExecutionMetrics{}, nil
	}

	ctx := context.NewContext()
	repairer.Repair(ctx, namespace, repairTimeRange, shard)
	require.Equal(t, namespace, resNamespace)
	require.Equal(t, resShard, shard)
	require.Equal(t, int64(2), resDiff.NumSeries)
	require.Equal(t, int64(3), resDiff.NumBlocks)
	require.Equal(t, 0, len(resDiff.ChecksumDifferences.Series()))
	sizeDiffSeries := resDiff.SizeDifferences.Series()
	require.Equal(t, 1, len(sizeDiffSeries))
	series, exists := sizeDiffSeries[ts.StringID("foo").Hash()]
	require.True(t, exists)
	blocks := series.Metadata.Blocks()
	require.Equal(t, 1, len(blocks))
	block, exists := blocks[now.Add(time.Hour)]
	require.True(t, exists)
	require.Equal(t, now.Add(time.Hour), block.Start())
	expected := []repair.HostBlockMetadata{
		{Host: topology.NewHost("0", "addr0"), Size: sizes[1], Checksum: &checksums[1]},
		{Host: topology.NewHost("1", "addr1"), Size: sizes[0], Checksum: &checksums[1]},
	}
	require.Equal(t, expected, block.Metadata())
}

func TestRepairerRepairTimes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	database := newMockDatabase()
	database.opts = database.opts.SetRepairOptions(testRepairOptions(ctrl))
	now := time.Unix(188000, 0)
	clockOpts := database.opts.ClockOptions()
	database.opts = database.opts.SetClockOptions(clockOpts.SetNowFn(func() time.Time { return now }))

	inputTimes := []struct {
		bs time.Time
		rs repairState
	}{
		{time.Unix(14400, 0), repairState{repairFailed, 2}},
		{time.Unix(28800, 0), repairState{repairFailed, 3}},
		{time.Unix(36000, 0), repairState{repairNotStarted, 0}},
		{time.Unix(43200, 0), repairState{repairSuccess, 1}},
	}
	repairer, err := newDatabaseRepairer(database)
	require.NoError(t, err)
	r := repairer.(*dbRepairer)
	for _, input := range inputTimes {
		r.repairStates[input.bs] = input.rs
	}

	res := r.repairTimeRanges()
	expectedRanges := xtime.NewRanges().
		AddRange(xtime.Range{Start: time.Unix(14400, 0), End: time.Unix(28800, 0)}).
		AddRange(xtime.Range{Start: time.Unix(50400, 0), End: time.Unix(187200, 0)})
	require.Equal(t, expectedRanges, res)
}

func TestRepairerRepairWithTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repairTimeRange := xtime.Range{Start: time.Unix(7200, 0), End: time.Unix(14400, 0)}
	database := newMockDatabase()
	database.opts = database.opts.SetRepairOptions(testRepairOptions(ctrl))
	repairer, err := newDatabaseRepairer(database)
	require.NoError(t, err)
	r := repairer.(*dbRepairer)

	inputs := []struct {
		name string
		err  error
	}{
		{"foo", errors.New("some error")},
		{"bar", errors.New("some other error")},
		{"baz", nil},
	}
	namespaces := make(map[string]databaseNamespace)
	for _, input := range inputs {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().Repair(gomock.Not(nil), repairTimeRange).Return(input.err)
		if input.err != nil {
			ns.EXPECT().ID().Return(ts.StringID(input.name))
		}
		namespaces[input.name] = ns
	}
	database.namespaces = namespaces

	require.Error(t, r.repairWithTimeRange(repairTimeRange))
}

func TestNewPendingReplicasMapComplexDiff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	database := newMockDatabase()
	database.opts = database.opts.SetRepairOptions(testRepairOptions(ctrl))
	repairer, err := newDatabaseRepairer(database)
	require.NoError(t, err)
	r := repairer.(*dbRepairer).shardRepairer.(shardRepairer)
	t0 := time.Now()
	sizes := []int64{1, 2, 1}
	checksums := []uint32{1, 1, 2}
	hostsMetadata := generateSampleHostsMetadata(t, t0, 1, sizes, checksums)
	repairOpts := testRepairOptions(ctrl)
	mcr := newTestMetadataComparisonResult(ctrl, t, hostsMetadata, 3, repairOpts)
	originHost := testHost(0)
	pendingReplicasMap, _ := r.newPendingReplicasMap(mcr, originHost)
	for _, replicas := range pendingReplicasMap {
		require.True(t, !replicas.contains(testHost(0)))
		require.True(t, replicas.contains(testHost(1)))
		require.True(t, replicas.contains(testHost(2)))
	}
}

func TestNewPendingReplicasMapSingleChecksumDiff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	database := newMockDatabase()
	database.opts = database.opts.SetRepairOptions(testRepairOptions(ctrl))
	repairer, err := newDatabaseRepairer(database)
	require.NoError(t, err)
	r := repairer.(*dbRepairer).shardRepairer.(shardRepairer)

	t0 := time.Now()
	sizes := []int64{1, 2, 1}
	checksums := []uint32{1, 1, 1}
	hostsMetadata := generateSampleHostsMetadata(t, t0, 1, sizes, checksums)
	repairOpts := testRepairOptions(ctrl)
	mcr := newTestMetadataComparisonResult(ctrl, t, hostsMetadata, 3, repairOpts)
	originHost := testHost(0)
	pendingReplicasMap, _ := r.newPendingReplicasMap(mcr, originHost)
	for _, replicas := range pendingReplicasMap {
		require.True(t, replicas.contains(testHost(1)))
		require.True(t, !replicas.contains(testHost(0)))
		require.True(t, !replicas.contains(testHost(2)))
	}
}

func TestNewPendingReplicasMapAllIdentical(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	database := newMockDatabase()
	database.opts = database.opts.SetRepairOptions(testRepairOptions(ctrl))
	repairer, err := newDatabaseRepairer(database)
	require.NoError(t, err)
	r := repairer.(*dbRepairer).shardRepairer.(shardRepairer)

	t0 := time.Now()
	sizes := []int64{1, 1, 1}
	checksums := []uint32{1, 1, 1}
	allPeersIdenticalHostsMetadata := generateSampleHostsMetadata(t, t0, 1, sizes, checksums)
	repairOpts := testRepairOptions(ctrl)
	mcr := newTestMetadataComparisonResult(ctrl, t, allPeersIdenticalHostsMetadata, 3, repairOpts)
	originHost := testHost(0)
	pendingReplicasMap, _ := r.newPendingReplicasMap(mcr, originHost)
	for _, replicas := range pendingReplicasMap {
		require.Empty(t, *replicas)
	}
}

func TestDatabaseShardRepairerRepairDifferencesAllSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		now           = time.Now()
		namespace     = ts.StringID("testNamespace")
		numTestSeries = 3
		numPeers      = 3
	)

	session := client.NewMockAdminSession(ctrl)
	session.EXPECT().Origin().Return(testHost(0))
	mockClient := client.NewMockAdminClient(ctrl)
	mockClient.EXPECT().DefaultAdminSession().Return(session, nil)
	rpOpts := testRepairOptions(ctrl).SetAdminClient(mockClient)
	nowFn := func() time.Time { return now }
	opts := testDatabaseOptions()
	copts := opts.ClockOptions()
	iopts := opts.InstrumentOptions()
	opts = opts.
		SetClockOptions(copts.SetNowFn(nowFn)).
		SetInstrumentOptions(iopts.SetMetricsScope(tally.NoopScope))

	shardID := uint32(0)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(shardID).AnyTimes()

	// this test considers repair in the following scenario:
	// input data:
	// - 3 hosts
	// - each host has 3 series, each with a single block
	// - series 0 is identical for all three hosts (both size and checksum)
	// - series 1 is identical amongst hosts 0 and 1; host 2 differs by size
	// - series 2 is identical amongst hosts 0 and 2; host 1 differs by checksum
	// communication:
	// - the local host (host 0) is able to retrieve all data blocks from other hosts
	// expected final status:
	// - the local host (host 0) has retrieved all different data from its peers and
	//   is fully 'repaired'
	sizes := []int64{
		1, 2, 3, // host 0 series' block sizes
		1, 2, 3, // host 1 series' block sizes
		1, 4, 3, // host 2 series' block sizes
	}
	checksums := []uint32{
		4, 5, 6, // host 0 series' block checksums
		4, 5, 1, // host 1 series' block checksums
		4, 5, 6, // host 2 series' block checksums
	}
	testHostInputData := generateSampleHostsMetadata(t, now, numTestSeries, sizes, checksums)
	metadataDiffRes := newTestMetadataComparisonResult(ctrl, t, testHostInputData, numPeers, rpOpts)

	// mock blocks and shards for [ series 2, host 1 ]
	ts2 := testSeries(2)
	blk21 := block.NewMockDatabaseBlock(ctrl)
	blk21.EXPECT().StartTime().Return(now)
	shard.EXPECT().UpdateSeries(ts2, blk21, true).Return(nil)

	// mock blocks and shards for [ series 1, host 2 ]
	ts1 := testSeries(1)
	blk12 := block.NewMockDatabaseBlock(ctrl)
	blk12.EXPECT().StartTime().Return(now)
	shard.EXPECT().UpdateSeries(ts1, blk12, true).Return(nil)

	// mock peerBlocksIter
	peerBlocksIter := client.NewMockPeerBlocksIter(ctrl)
	gomock.InOrder(
		peerBlocksIter.EXPECT().Next().Return(true),
		peerBlocksIter.EXPECT().Current().Return(testHost(1), ts2, blk21),
		peerBlocksIter.EXPECT().Next().Return(true),
		peerBlocksIter.EXPECT().Current().Return(testHost(2), ts1, blk12),
		peerBlocksIter.EXPECT().Next().Return(false),
		peerBlocksIter.EXPECT().Err().Return(nil),
	)

	session.EXPECT().
		FetchRepairBlocksFromPeers(namespace, shardID, gomock.Any(), gomock.Any()).
		Return(peerBlocksIter, nil)

	databaseShardRepairer, err := newShardRepairer(opts, rpOpts)
	require.NoError(t, err)
	repairFn := databaseShardRepairer.(shardRepairer).repairFn
	execMetrics, err := repairFn(namespace, shard, metadataDiffRes)
	require.NoError(t, err)
	require.Equal(t, int64(2), execMetrics.NumRequested)
	require.Equal(t, int64(2), execMetrics.NumRepaired)
	require.Equal(t, int64(0), execMetrics.NumFailed)
	require.Equal(t, int64(0), execMetrics.NumPending)
}

func TestDatabaseShardRepairerRepairDifferencesNetworkFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		now           = time.Now()
		namespace     = ts.StringID("testNamespace")
		numTestSeries = 3
		numPeers      = 3
	)

	session := client.NewMockAdminSession(ctrl)
	session.EXPECT().Origin().Return(testHost(0))
	mockClient := client.NewMockAdminClient(ctrl)
	mockClient.EXPECT().DefaultAdminSession().Return(session, nil)
	rpOpts := testRepairOptions(ctrl).SetAdminClient(mockClient)
	nowFn := func() time.Time { return now }
	opts := testDatabaseOptions()
	copts := opts.ClockOptions()
	iopts := opts.InstrumentOptions()
	opts = opts.
		SetClockOptions(copts.SetNowFn(nowFn)).
		SetInstrumentOptions(iopts.SetMetricsScope(tally.NoopScope))

	shardID := uint32(0)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(shardID).AnyTimes()

	// this test considers repair in the following scenario:
	// input data:
	// - 3 hosts
	// - each host has 3 series, each with a single block
	// - series 0 is identical for all three hosts (both size and checksum)
	// - series 1 is identical amongst hosts 0 and 1; host 2 differs by size
	// - series 2 is identical amongst hosts 0 and 2; host 1 differs by checksum
	// communication:
	// - the local host (host 0) is able to retrieve data blocks from host 1, but not host 2
	// expected final status:
	// - the local host (host 0) has repaired retrieved data, and indicates error
	sizes := []int64{
		1, 2, 3, // host 0 series' block sizes
		1, 2, 3, // host 1 series' block sizes
		1, 4, 3, // host 2 series' block sizes
	}
	checksums := []uint32{
		4, 5, 6, // host 0 series' block checksums
		4, 5, 1, // host 1 series' block checksums
		4, 5, 6, // host 2 series' block checksums
	}
	testHostInputData := generateSampleHostsMetadata(t, now, numTestSeries, sizes, checksums)
	metadataDiffRes := newTestMetadataComparisonResult(ctrl, t, testHostInputData, numPeers, rpOpts)

	// mock blocks and shards for [ series 2, host 1 ]
	ts2 := testSeries(2)
	blk21 := block.NewMockDatabaseBlock(ctrl)
	blk21.EXPECT().StartTime().Return(now)
	shard.EXPECT().UpdateSeries(ts2, blk21, true).Return(nil)

	// mock peerBlocksIter
	peerBlocksIter := client.NewMockPeerBlocksIter(ctrl)
	gomock.InOrder(
		peerBlocksIter.EXPECT().Next().Return(true),
		peerBlocksIter.EXPECT().Current().Return(testHost(1), ts2, blk21),
		peerBlocksIter.EXPECT().Next().Return(false),
		peerBlocksIter.EXPECT().Err().Return(nil),
	)

	session.EXPECT().
		FetchRepairBlocksFromPeers(namespace, shardID, gomock.Any(), gomock.Any()).
		Return(peerBlocksIter, nil)

	databaseShardRepairer, err := newShardRepairer(opts, rpOpts)
	require.NoError(t, err)
	repairFn := databaseShardRepairer.(shardRepairer).repairFn
	execMetrics, err := repairFn(namespace, shard, metadataDiffRes)
	require.Error(t, err)
	require.Equal(t, int64(2), execMetrics.NumRequested)
	require.Equal(t, int64(1), execMetrics.NumRepaired)
	require.Equal(t, int64(0), execMetrics.NumFailed)
	require.Equal(t, int64(1), execMetrics.NumPending)
}

func TestDatabaseShardRepairerRepairDifferencesUpdateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		now           = time.Now()
		namespace     = ts.StringID("testNamespace")
		numTestSeries = 3
		numPeers      = 3
	)

	session := client.NewMockAdminSession(ctrl)
	session.EXPECT().Origin().Return(testHost(0))
	mockClient := client.NewMockAdminClient(ctrl)
	mockClient.EXPECT().DefaultAdminSession().Return(session, nil)
	rpOpts := testRepairOptions(ctrl).SetAdminClient(mockClient)
	nowFn := func() time.Time { return now }
	opts := testDatabaseOptions()
	copts := opts.ClockOptions()
	iopts := opts.InstrumentOptions()
	opts = opts.
		SetClockOptions(copts.SetNowFn(nowFn)).
		SetInstrumentOptions(iopts.SetMetricsScope(tally.NoopScope))

	shardID := uint32(0)
	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(shardID).AnyTimes()

	// this test considers repair in the following scenario:
	// input data:
	// - 3 hosts
	// - each host has 3 series, each with a single block
	// - series 0 is identical for all three hosts (both size and checksum)
	// - series 1 is identical amongst hosts 0 and 1; host 2 differs by size
	// - series 2 is identical amongst hosts 0 and 2; host 1 differs by checksum
	// communication:
	// - the local host (host 0) is able to retrieve data blocks from both hosts
	// updates:
	// - the local host (host 0) is unable to update the block received from host 1
	// expected final status:
	// - the local host (host 0) has repaired partially, and indicates error
	sizes := []int64{
		1, 2, 3, // host 0 series' block sizes
		1, 2, 3, // host 1 series' block sizes
		1, 4, 3, // host 2 series' block sizes
	}
	checksums := []uint32{
		4, 5, 6, // host 0 series' block checksums
		4, 5, 1, // host 1 series' block checksums
		4, 5, 6, // host 2 series' block checksums
	}
	testHostInputData := generateSampleHostsMetadata(t, now, numTestSeries, sizes, checksums)
	metadataDiffRes := newTestMetadataComparisonResult(ctrl, t, testHostInputData, numPeers, rpOpts)

	// mock blocks and shards for [ series 2, host 1 ]
	ts2 := testSeries(2)
	blk21 := block.NewMockDatabaseBlock(ctrl)
	blk21.EXPECT().StartTime().Return(now)
	shard.EXPECT().UpdateSeries(ts2, blk21, true).Return(fmt.Errorf("simulated failure"))

	// mock blocks and shards for [ series 1, host 2 ]
	ts1 := testSeries(1)
	blk12 := block.NewMockDatabaseBlock(ctrl)
	blk12.EXPECT().StartTime().Return(now)
	shard.EXPECT().UpdateSeries(ts1, blk12, true).Return(nil)

	// mock peerBlocksIter
	peerBlocksIter := client.NewMockPeerBlocksIter(ctrl)
	gomock.InOrder(
		peerBlocksIter.EXPECT().Next().Return(true),
		peerBlocksIter.EXPECT().Current().Return(testHost(1), ts2, blk21),
		peerBlocksIter.EXPECT().Next().Return(true),
		peerBlocksIter.EXPECT().Current().Return(testHost(2), ts1, blk12),
		peerBlocksIter.EXPECT().Next().Return(false),
		peerBlocksIter.EXPECT().Err().Return(nil),
	)

	session.EXPECT().
		FetchRepairBlocksFromPeers(namespace, shardID, gomock.Any(), gomock.Any()).
		Return(peerBlocksIter, nil)

	databaseShardRepairer, err := newShardRepairer(opts, rpOpts)
	require.NoError(t, err)
	repairFn := databaseShardRepairer.(shardRepairer).repairFn
	execMetrics, err := repairFn(namespace, shard, metadataDiffRes)
	require.Error(t, err)
	require.Equal(t, int64(2), execMetrics.NumRequested)
	require.Equal(t, int64(1), execMetrics.NumRepaired)
	require.Equal(t, int64(1), execMetrics.NumFailed)
	require.Equal(t, int64(0), execMetrics.NumPending)
}

func generateSampleHostsMetadata(
	t *testing.T,
	t0 time.Time,
	numSeries int,
	sizes []int64,
	checksums []uint32,
) []testHostMetadata {
	require.Equal(t, len(sizes), len(checksums))
	require.Equal(t, len(sizes)%numSeries, 0)
	numPeers := len(sizes) / numSeries
	thm := []testHostMetadata{}
	for i := 0; i < numPeers; i++ {
		md := []block.BlocksMetadata{}
		for j := 0; j < numSeries; j++ {
			md = append(md, block.BlocksMetadata{
				ID: testSeries(j),
				Blocks: []block.Metadata{
					block.Metadata{
						Start:    t0,
						Size:     sizes[i*numSeries+j],
						Checksum: &checksums[i*numSeries+j],
					},
				},
			})
		}
		thm = append(thm, testHostMetadata{
			host:     testHost(i),
			metadata: md,
		})
	}
	return thm
}

type testHostMetadata struct {
	host     topology.Host
	metadata []block.BlocksMetadata
}

func testHost(i int) topology.Host {
	return topology.NewHost(strconv.Itoa(i), fmt.Sprintf("testhost%d", i))
}

func testSeries(i int) ts.ID {
	return ts.StringID(fmt.Sprintf("foo%d", i))
}

func newTestMetadataComparisonResult(
	ctrl *gomock.Controller,
	t *testing.T,
	hostsMetadata []testHostMetadata,
	numReplicas int,
	opts repair.Options,
) repair.MetadataComparisonResult {
	comparer := repair.NewReplicaMetadataComparer(numReplicas, opts)
	localIter := block.NewMockFilteredBlocksMetadataIter(ctrl)
	localIterCalls := []*gomock.Call{}
	peerIter := client.NewMockPeerBlocksMetadataIter(ctrl)
	peerIterCalls := []*gomock.Call{}
	for i, hm := range hostsMetadata {
		if i == 0 { // localhost
			for _, md := range hm.metadata {
				for _, block := range md.Blocks {
					localIterCalls = append(localIterCalls,
						localIter.EXPECT().Next().Return(true),
						localIter.EXPECT().Current().Return(md.ID, block))
				}
			}
		} else { // peer
			for _, md := range hm.metadata {
				peerIterCalls = append(peerIterCalls,
					peerIter.EXPECT().Next().Return(true),
					peerIter.EXPECT().Current().Return(hm.host, md))
			}
		}
	}
	localIterCalls = append(localIterCalls,
		localIter.EXPECT().Next().Return(false))
	gomock.InOrder(localIterCalls...)
	comparer.AddLocalMetadata(hostsMetadata[0].host, localIter)

	peerIterCalls = append(peerIterCalls,
		peerIter.EXPECT().Next().Return(false),
		peerIter.EXPECT().Err().Return(nil))
	gomock.InOrder(peerIterCalls...)
	comparer.AddPeerMetadata(peerIter)
	return comparer.Compare()
}
