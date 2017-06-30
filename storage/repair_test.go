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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/topology"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestDatabaseRepairerStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDatabase := newMockDatabase(t)
	mockDatabase.opts = mockDatabase.opts.SetRepairOptions(testRepairOptions(ctrl))

	databaseRepairer, err := newDatabaseRepairer(mockDatabase, mockDatabase.opts)
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
	mockDatabase := newMockDatabase(t)
	clockOpts := mockDatabase.opts.ClockOptions().SetNowFn(nowFn)
	repairOpts := testRepairOptions(ctrl).
		SetRepairInterval(repairInterval).
		SetRepairTimeOffset(repairTimeOffset)
	mockDatabase.opts = mockDatabase.opts.
		SetClockOptions(clockOpts.SetNowFn(nowFn)).
		SetRepairOptions(repairOpts)

	databaseRepairer, err := newDatabaseRepairer(mockDatabase, mockDatabase.opts)
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

	mockDatabase := newMockDatabase(t)
	clockOpts := mockDatabase.opts.ClockOptions().SetNowFn(nowFn)
	repairOpts := testRepairOptions(ctrl).
		SetRepairInterval(repairInterval).
		SetRepairTimeOffset(repairTimeOffset)
	mockDatabase.opts = mockDatabase.opts.
		SetClockOptions(clockOpts.SetNowFn(nowFn)).
		SetRepairOptions(repairOpts)

	databaseRepairer, err := newDatabaseRepairer(mockDatabase, mockDatabase.opts)
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

	mockDatabase := newMockDatabase(t)
	mockDatabase.opts = mockDatabase.opts.SetRepairOptions(testRepairOptions(ctrl))

	databaseRepairer, err := newDatabaseRepairer(mockDatabase, mockDatabase.opts)
	require.NoError(t, err)
	repairer := databaseRepairer.(*dbRepairer)

	mockDatabase.bs = bootstrapNotStarted
	require.Nil(t, repairer.Repair())
}

func TestDatabaseShardRepairerRepair(t *testing.T) {
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
	opts := testDatabaseOptions(t)
	copts := opts.ClockOptions()
	iopts := opts.InstrumentOptions()
	rtopts := defaultTestRetentionOpts
	opts = opts.
		SetClockOptions(copts.SetNowFn(nowFn)).
		SetInstrumentOptions(iopts.SetMetricsScope(tally.NoopScope))

	var (
		namespace       = ts.StringID("testNamespace")
		start           = now
		end             = now.Add(rtopts.BlockSize())
		repairTimeRange = xtime.Range{Start: start, End: end}
		fetchOpts       = block.FetchBlocksMetadataOptions{
			IncludeSizes:     true,
			IncludeChecksums: true,
			IncludeLastRead:  false,
		}
	)

	sizes := []int64{1, 2, 3}
	checksums := []uint32{4, 5, 6}
	lastRead := now.Add(-time.Minute)
	shardID := uint32(0)
	shard := NewMockdatabaseShard(ctrl)

	expectedResults := block.NewFetchBlocksMetadataResults()
	results := block.NewFetchBlockMetadataResults()
	results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute),
		sizes[0], &checksums[0], lastRead, nil))
	results.Add(block.NewFetchBlockMetadataResult(now.Add(time.Hour),
		sizes[1], &checksums[1], lastRead, nil))
	expectedResults.Add(block.NewFetchBlocksMetadataResult(ts.StringID("foo"), results))
	results = block.NewFetchBlockMetadataResults()
	results.Add(block.NewFetchBlockMetadataResult(now.Add(30*time.Minute),
		sizes[2], &checksums[2], lastRead, nil))
	expectedResults.Add(block.NewFetchBlocksMetadataResult(ts.StringID("bar"), results))

	any := gomock.Any()
	shard.EXPECT().
		FetchBlocksMetadata(any, start, end, any, int64(0), fetchOpts).
		Return(expectedResults, nil)
	shard.EXPECT().ID().Return(shardID).AnyTimes()

	peerIter := client.NewMockPeerBlocksMetadataIter(ctrl)
	inputBlocks := []struct {
		host topology.Host
		meta block.BlocksMetadata
	}{
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata(ts.StringID("foo"), []block.Metadata{
			block.NewMetadata(now.Add(30*time.Minute), sizes[0], &checksums[0], lastRead),
			block.NewMetadata(now.Add(time.Hour), sizes[0], &checksums[1], lastRead),
		})},
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata(ts.StringID("bar"), []block.Metadata{
			block.NewMetadata(now.Add(30*time.Minute), sizes[2], &checksums[2], lastRead),
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
	repairer.recordFn = func(namespace ts.ID, shard databaseShard, diffRes repair.MetadataComparisonResult) {
		resNamespace = namespace
		resShard = shard
		resDiff = diffRes
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

	database := newMockDatabase(t)
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
	repairer, err := newDatabaseRepairer(database, database.opts)
	require.NoError(t, err)
	r := repairer.(*dbRepairer)
	for _, input := range inputTimes {
		r.repairStates.set(defaultTestNs1ID, input.bs, input.rs)
	}

	testNs := newTestNamespace(t)
	res := r.namespaceRepairTimeRanges(testNs)
	expectedRanges := xtime.NewRanges().
		AddRange(xtime.Range{Start: time.Unix(14400, 0), End: time.Unix(28800, 0)}).
		AddRange(xtime.Range{Start: time.Unix(36000, 0), End: time.Unix(43200, 0)}).
		AddRange(xtime.Range{Start: time.Unix(50400, 0), End: time.Unix(187200, 0)})
	require.Equal(t, expectedRanges, res)
}

func TestRepairerRepairWithTime(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	repairTimeRange := xtime.Range{Start: time.Unix(7200, 0), End: time.Unix(14400, 0)}
	database := newMockDatabase(t)
	database.opts = database.opts.SetRepairOptions(testRepairOptions(ctrl))
	repairer, err := newDatabaseRepairer(database, database.opts)
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
		id := ts.StringID(input.name)
		ropts := retention.NewOptions()
		nsOpts := namespace.NewMockOptions(ctrl)
		nsOpts.EXPECT().RetentionOptions().Return(ropts)
		ns.EXPECT().Options().Return(nsOpts)
		ns.EXPECT().Repair(gomock.Not(nil), repairTimeRange).Return(input.err)
		ns.EXPECT().ID().Return(id).AnyTimes()
		err := r.repairNamespaceWithTimeRange(ns, repairTimeRange)
		rs, ok := r.repairStates.get(id, time.Unix(7200, 0))
		require.True(t, ok)
		if input.err == nil {
			require.NoError(t, err)
			require.Equal(t, repairState{Status: repairSuccess}, rs)
		} else {
			require.Error(t, err)
			require.Equal(t, repairState{Status: repairFailed, NumFailures: 1}, rs)
		}
		namespaces[input.name] = ns
	}
	database.namespaces = namespaces
}

func TestRepairerTimesMultipleNamespaces(t *testing.T) {
	// tf2(i) returns the start time of the i_th 2 hour block since epoch
	tf2 := func(i int) time.Time {
		return time.Unix(int64(i*7200), 0)
	}
	// tf4(i) returns the start time of the i_th 4 hour block since epoch
	tf4 := func(i int) time.Time {
		return tf2(2 * i)
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	database := newMockDatabase(t)
	database.opts = database.opts.SetRepairOptions(testRepairOptions(ctrl))

	now := time.Unix(188000, 0)
	clockOpts := database.opts.ClockOptions()
	database.opts = database.opts.SetClockOptions(clockOpts.SetNowFn(func() time.Time { return now }))

	inputTimes := []struct {
		ns ts.ID
		bs time.Time
		rs repairState
	}{
		{defaultTestNs1ID, tf2(2), repairState{repairFailed, 2}},
		{defaultTestNs1ID, tf2(4), repairState{repairFailed, 3}},
		{defaultTestNs1ID, tf2(5), repairState{repairNotStarted, 0}},
		{defaultTestNs1ID, tf2(6), repairState{repairSuccess, 1}},
		{defaultTestNs2ID, tf4(1), repairState{repairFailed, 1}},
		{defaultTestNs2ID, tf4(2), repairState{repairFailed, 3}},
		{defaultTestNs2ID, tf4(4), repairState{repairNotStarted, 0}},
		{defaultTestNs2ID, tf4(6), repairState{repairSuccess, 1}},
	}
	repairer, err := newDatabaseRepairer(database, database.opts)
	require.NoError(t, err)
	r := repairer.(*dbRepairer)
	for _, input := range inputTimes {
		r.repairStates.set(input.ns, input.bs, input.rs)
	}

	testNs1 := newTestNamespaceWithIDOpts(t, defaultTestNs1ID, defaultTestNs1Opts)
	res := r.namespaceRepairTimeRanges(testNs1)
	expectedRanges := xtime.NewRanges().
		AddRange(xtime.Range{Start: tf2(2), End: tf2(4)}).
		AddRange(xtime.Range{Start: tf2(5), End: tf2(6)}).
		AddRange(xtime.Range{Start: tf2(7), End: tf2(26)})
	require.Equal(t, expectedRanges, res)

	testNs2 := newTestNamespaceWithIDOpts(t, defaultTestNs2ID, defaultTestNs2Opts)
	res = r.namespaceRepairTimeRanges(testNs2)
	expectedRanges = xtime.NewRanges().
		AddRange(xtime.Range{Start: tf4(1), End: tf4(2)}).
		AddRange(xtime.Range{Start: tf4(3), End: tf4(6)}).
		AddRange(xtime.Range{Start: tf4(7), End: tf4(13)})
	require.Equal(t, expectedRanges, res)
}
