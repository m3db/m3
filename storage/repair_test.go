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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/repair"
	"github.com/m3db/m3db/topology"

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

func TestShardRepairerRepair(t *testing.T) {
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
		namespace = "testNamespace"
		start     = now.Add(-rtopts.RetentionPeriod())
		end       = now.Add(-rtopts.BufferPast()).Add(-rtopts.BlockSize())
	)

	sizes := []int64{1, 2, 3}
	checksums := []uint32{4, 5, 6}
	shardID := uint32(0)
	shard := NewMockdatabaseShard(ctrl)

	shard.EXPECT().
		FetchBlocksMetadata(gomock.Any(), gomock.Any(), int64(0), true, true).
		Return([]block.FetchBlocksMetadataResult{
			block.NewFetchBlocksMetadataResult("foo", []block.FetchBlockMetadataResult{
				block.NewFetchBlockMetadataResult(now.Add(-4*time.Hour), &sizes[0], &checksums[0], nil),
				block.NewFetchBlockMetadataResult(now.Add(-6*time.Hour), &sizes[1], &checksums[1], nil),
			}),
			block.NewFetchBlocksMetadataResult("bar", []block.FetchBlockMetadataResult{
				block.NewFetchBlockMetadataResult(now.Add(-4*time.Hour), &sizes[2], &checksums[2], nil),
			}),
		}, nil)
	shard.EXPECT().ID().Return(shardID).AnyTimes()

	peerIter := client.NewMockPeerBlocksMetadataIter(ctrl)
	inputBlocks := []struct {
		host topology.Host
		meta block.BlocksMetadata
	}{
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata("foo", []block.Metadata{
			block.NewMetadata(now.Add(-4*time.Hour), sizes[0], &checksums[0]),
			block.NewMetadata(now.Add(-6*time.Hour), sizes[0], &checksums[1]),
		})},
		{topology.NewHost("1", "addr1"), block.NewBlocksMetadata("bar", []block.Metadata{
			block.NewMetadata(now.Add(-4*time.Hour), sizes[2], &checksums[2]),
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
		FetchBlocksMetadataFromPeers(namespace, shardID, start, end, rtopts.BlockSize()).
		Return(peerIter, nil)

	var (
		resNamespace string
		resShard     databaseShard
		resDiff      repair.MetadataComparisonResult
	)

	databaseShardRepairer, err := newShardRepairer(opts, rpOpts)
	require.NoError(t, err)
	repairer := databaseShardRepairer.(shardRepairer)
	repairer.recordFn = func(namespace string, shard databaseShard, diffRes repair.MetadataComparisonResult) {
		resNamespace = namespace
		resShard = shard
		resDiff = diffRes
	}

	repairer.Repair(namespace, shard)
	require.Equal(t, namespace, resNamespace)
	require.Equal(t, resShard, shard)
	require.Equal(t, int64(2), resDiff.NumSeries)
	require.Equal(t, int64(3), resDiff.NumBlocks)
	require.Equal(t, 0, len(resDiff.ChecksumDifferences.Series()))
	sizeDiffSeries := resDiff.SizeDifferences.Series()
	require.Equal(t, 1, len(sizeDiffSeries))
	series, exists := sizeDiffSeries["foo"]
	require.True(t, exists)
	blocks := series.Blocks()
	require.Equal(t, 1, len(blocks))
	block, exists := blocks[now.Add(-6*time.Hour)]
	require.True(t, exists)
	require.Equal(t, now.Add(-6*time.Hour), block.Start())
	expected := []repair.HostBlockMetadata{
		{topology.NewHost("0", "addr0"), sizes[1], &checksums[1]},
		{topology.NewHost("1", "addr1"), sizes[0], &checksums[1]},
	}
	require.Equal(t, expected, block.Metadata())
}
