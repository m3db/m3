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
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/storage/series/lookup"
	"github.com/m3db/m3/src/dbnode/ts"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type testIncreasingIndex struct {
	created uint64
}

func (i *testIncreasingIndex) nextIndex() uint64 {
	created := atomic.AddUint64(&i.created, 1)
	return created - 1
}

func testDatabaseShard(t *testing.T, opts Options) *dbShard {
	return testDatabaseShardWithIndexFn(t, opts, nil)
}

func testDatabaseShardWithIndexFn(
	t *testing.T,
	opts Options,
	idx namespaceIndex,
) *dbShard {
	metadata, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	nsReaderMgr := newNamespaceReaderManager(metadata, tally.NoopScope, opts)
	seriesOpts := NewSeriesOptionsFromOptions(opts, defaultTestNs1Opts.RetentionOptions())
	return newDatabaseShard(metadata, 0, nil, nsReaderMgr,
		&testIncreasingIndex{}, commitLogWriteNoOp, idx, true, opts, seriesOpts).(*dbShard)
}

func addMockSeries(ctrl *gomock.Controller, shard *dbShard, id ident.ID, tags ident.Tags, index uint64) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().Return(id).AnyTimes()
	series.EXPECT().Tags().Return(tags).AnyTimes()
	series.EXPECT().IsEmpty().Return(false).AnyTimes()
	shard.Lock()
	shard.insertNewShardEntryWithLock(lookup.NewEntry(series, index))
	shard.Unlock()
	return series
}

func TestShardDontNeedBootstrap(t *testing.T) {
	opts := testDatabaseOptions()
	testNs, closer := newTestNamespace(t)
	defer closer()
	seriesOpts := NewSeriesOptionsFromOptions(opts, testNs.Options().RetentionOptions())
	shard := newDatabaseShard(testNs.metadata, 0, nil, nil,
		&testIncreasingIndex{}, commitLogWriteNoOp, nil, false, opts, seriesOpts).(*dbShard)
	defer shard.Close()

	require.Equal(t, Bootstrapped, shard.bootstrapState)
	require.True(t, shard.newSeriesBootstrapped)
}

func TestShardBootstrapState(t *testing.T) {
	opts := testDatabaseOptions()
	testNs, closer := newTestNamespace(t)
	defer closer()
	seriesOpts := NewSeriesOptionsFromOptions(opts, testNs.Options().RetentionOptions())
	shard := newDatabaseShard(testNs.metadata, 0, nil, nil,
		&testIncreasingIndex{}, commitLogWriteNoOp, nil, false, opts, seriesOpts).(*dbShard)
	defer shard.Close()

	require.Equal(t, Bootstrapped, shard.bootstrapState)
	require.Equal(t, Bootstrapped, shard.BootstrapState())
}

func TestShardFlushStateNotStarted(t *testing.T) {
	now := time.Now()
	nowFn := func() time.Time {
		return now
	}

	opts := testDatabaseOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	ropts := defaultTestRetentionOpts
	earliest, latest := retention.FlushTimeStart(ropts, now), retention.FlushTimeEnd(ropts, now)

	s := testDatabaseShard(t, opts)
	defer s.Close()

	notStarted := fileOpState{Status: fileOpNotStarted}
	for st := earliest; !st.After(latest); st = st.Add(ropts.BlockSize()) {
		assert.Equal(t, notStarted, s.FlushState(earliest))
	}
}

func TestShardBootstrapWithError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	s := testDatabaseShard(t, opts)
	defer s.Close()

	fooSeries := series.NewMockDatabaseSeries(ctrl)
	fooSeries.EXPECT().ID().Return(ident.StringID("foo")).AnyTimes()
	fooSeries.EXPECT().IsEmpty().Return(false).AnyTimes()
	barSeries := series.NewMockDatabaseSeries(ctrl)
	barSeries.EXPECT().ID().Return(ident.StringID("bar")).AnyTimes()
	barSeries.EXPECT().IsEmpty().Return(false).AnyTimes()
	s.Lock()
	s.insertNewShardEntryWithLock(lookup.NewEntry(fooSeries, 0))
	s.insertNewShardEntryWithLock(lookup.NewEntry(barSeries, 0))
	s.Unlock()

	fooBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	barBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	fooSeries.EXPECT().Bootstrap(fooBlocks).Return(series.BootstrapResult{}, nil)
	fooSeries.EXPECT().IsBootstrapped().Return(true)
	barSeries.EXPECT().Bootstrap(barBlocks).Return(series.BootstrapResult{}, errors.New("series error"))
	barSeries.EXPECT().IsBootstrapped().Return(true)

	fooID := ident.StringID("foo")
	barID := ident.StringID("bar")

	bootstrappedSeries := result.NewMap(result.MapOptions{})
	bootstrappedSeries.Set(fooID, result.DatabaseSeriesBlocks{ID: fooID, Blocks: fooBlocks})
	bootstrappedSeries.Set(barID, result.DatabaseSeriesBlocks{ID: barID, Blocks: barBlocks})

	err := s.Bootstrap(bootstrappedSeries)

	require.NotNil(t, err)
	require.Equal(t, "series error", err.Error())
	require.Equal(t, Bootstrapped, s.bootstrapState)
}

func TestShardFlushDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapping
	err := s.Flush(time.Now(), nil)
	require.Equal(t, err, errShardNotBootstrappedToFlush)
}

func TestShardFlushSeriesFlushError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockStart := time.Unix(21600, 0)

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapped
	s.flushState.statesByTime[xtime.ToUnixNano(blockStart)] = fileOpState{
		Status:      fileOpFailed,
		NumFailures: 1,
	}

	var closed bool
	flush := persist.NewMockDataFlush(ctrl)
	prepared := persist.PreparedDataPersist{
		Persist: func(ident.ID, ident.Tags, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}
	prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.shard,
		BlockStart:        blockStart,
	})
	flush.EXPECT().PrepareData(prepareOpts).Return(prepared, nil)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		var expectedErr error
		if i == 1 {
			expectedErr = errors.New("error bar")
		}
		curr := series.NewMockDatabaseSeries(ctrl)
		curr.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		curr.EXPECT().IsEmpty().Return(false).AnyTimes()
		curr.EXPECT().
			Flush(gomock.Any(), blockStart, gomock.Any()).
			Do(func(context.Context, time.Time, persist.DataFn) {
				flushed[i] = struct{}{}
			}).
			Return(series.FlushOutcomeErr, expectedErr)
		s.list.PushBack(lookup.NewEntry(curr, 0))
	}

	err := s.Flush(blockStart, flush)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.NotNil(t, err)
	require.Equal(t, "error bar", err.Error())

	flushState := s.FlushState(blockStart)
	require.Equal(t, fileOpState{
		Status:      fileOpFailed,
		NumFailures: 2,
	}, flushState)
}

func TestShardFlushSeriesFlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockStart := time.Unix(21600, 0)

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapped
	s.flushState.statesByTime[xtime.ToUnixNano(blockStart)] = fileOpState{
		Status:      fileOpFailed,
		NumFailures: 1,
	}

	var closed bool
	flush := persist.NewMockDataFlush(ctrl)
	prepared := persist.PreparedDataPersist{
		Persist: func(ident.ID, ident.Tags, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}

	prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.shard,
		BlockStart:        blockStart,
	})
	flush.EXPECT().PrepareData(prepareOpts).Return(prepared, nil)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		curr := series.NewMockDatabaseSeries(ctrl)
		curr.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		curr.EXPECT().IsEmpty().Return(false).AnyTimes()
		curr.EXPECT().
			Flush(gomock.Any(), blockStart, gomock.Any()).
			Do(func(context.Context, time.Time, persist.DataFn) {
				flushed[i] = struct{}{}
			}).
			Return(series.FlushOutcomeFlushedToDisk, nil)
		s.list.PushBack(lookup.NewEntry(curr, 0))
	}

	err := s.Flush(blockStart, flush)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.Nil(t, err)

	flushState := s.FlushState(blockStart)
	require.Equal(t, fileOpState{
		Status:      fileOpSuccess,
		NumFailures: 0,
	}, flushState)
}

func TestShardSnapshotShardNotBootstrapped(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockStart := time.Unix(21600, 0)

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapping

	flush := persist.NewMockDataFlush(ctrl)
	err := s.Snapshot(blockStart, blockStart, flush)
	require.Equal(t, errShardNotBootstrappedToSnapshot, err)
}

func TestShardSnapshotSeriesSnapshotSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockStart := time.Unix(21600, 0)

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapped

	var closed bool
	flush := persist.NewMockDataFlush(ctrl)
	prepared := persist.PreparedDataPersist{
		Persist: func(ident.ID, ident.Tags, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}

	prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.shard,
		BlockStart:        blockStart,
		FileSetType:       persist.FileSetSnapshotType,
		Snapshot: persist.DataPrepareSnapshotOptions{
			SnapshotTime: blockStart,
		},
	})
	flush.EXPECT().PrepareData(prepareOpts).Return(prepared, nil)

	snapshotted := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		series := series.NewMockDatabaseSeries(ctrl)
		series.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		series.EXPECT().IsEmpty().Return(false).AnyTimes()
		series.EXPECT().
			Snapshot(gomock.Any(), blockStart, gomock.Any()).
			Do(func(context.Context, time.Time, persist.DataFn) {
				snapshotted[i] = struct{}{}
			}).
			Return(nil)
		s.list.PushBack(lookup.NewEntry(series, 0))
	}

	err := s.Snapshot(blockStart, blockStart, flush)

	require.Equal(t, len(snapshotted), 2)
	for i := 0; i < 2; i++ {
		_, ok := snapshotted[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.Nil(t, err)
}

func addMockTestSeries(ctrl *gomock.Controller, shard *dbShard, id ident.ID) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().AnyTimes().Return(id)
	shard.Lock()
	shard.insertNewShardEntryWithLock(lookup.NewEntry(series, 0))
	shard.Unlock()
	return series
}

func addTestSeries(shard *dbShard, id ident.ID) series.DatabaseSeries {
	return addTestSeriesWithCount(shard, id, 0)
}

func addTestSeriesWithCount(shard *dbShard, id ident.ID, count int32) series.DatabaseSeries {
	series := series.NewDatabaseSeries(id, ident.Tags{}, shard.seriesOpts)
	series.Bootstrap(nil)
	shard.Lock()
	entry := lookup.NewEntry(series, 0)
	for i := int32(0); i < count; i++ {
		entry.IncrementReaderWriterCount()
	}
	shard.insertNewShardEntryWithLock(entry)
	shard.Unlock()
	return series
}

func TestShardTick(t *testing.T) {
	now := time.Now()
	nowLock := sync.RWMutex{}
	nowFn := func() time.Time {
		nowLock.RLock()
		value := now
		nowLock.RUnlock()
		return value
	}
	setNow := func(t time.Time) {
		nowLock.Lock()
		now = t
		nowLock.Unlock()
	}

	opts := testDatabaseOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	earliestFlush := retention.FlushTimeStart(defaultTestRetentionOpts, now)
	beforeEarliestFlush := earliestFlush.Add(-defaultTestRetentionOpts.BlockSize())

	sleepPerSeries := time.Microsecond

	shard := testDatabaseShard(t, opts)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetTickPerSeriesSleepDuration(sleepPerSeries).
		SetTickSeriesBatchSize(1))
	defer shard.Close()

	// Also check that it expires flush states by time
	shard.flushState.statesByTime[xtime.ToUnixNano(earliestFlush)] = fileOpState{
		Status: fileOpSuccess,
	}
	shard.flushState.statesByTime[xtime.ToUnixNano(beforeEarliestFlush)] = fileOpState{
		Status: fileOpSuccess,
	}
	assert.Equal(t, 2, len(shard.flushState.statesByTime))

	var slept time.Duration
	shard.sleepFn = func(t time.Duration) {
		slept += t
		setNow(nowFn().Add(t))
	}

	ctx := context.NewContext()
	defer ctx.Close()

	shard.Write(ctx, ident.StringID("foo"), nowFn(), 1.0, xtime.Second, nil)
	shard.Write(ctx, ident.StringID("bar"), nowFn(), 2.0, xtime.Second, nil)
	shard.Write(ctx, ident.StringID("baz"), nowFn(), 3.0, xtime.Second, nil)

	r, err := shard.Tick(context.NewNoOpCanncellable(), nowFn())
	require.NoError(t, err)
	require.Equal(t, 3, r.activeSeries)
	require.Equal(t, 0, r.expiredSeries)
	require.Equal(t, 2*sleepPerSeries, slept) // Never sleeps on the first series

	// Ensure flush states by time was expired correctly
	require.Equal(t, 1, len(shard.flushState.statesByTime))
	_, ok := shard.flushState.statesByTime[xtime.ToUnixNano(earliestFlush)]
	require.True(t, ok)
}

func TestShardWriteAsync(t *testing.T) {
	testReporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)
	defer closer.Close()

	now := time.Now()
	nowLock := sync.RWMutex{}
	nowFn := func() time.Time {
		nowLock.RLock()
		value := now
		nowLock.RUnlock()
		return value
	}
	setNow := func(t time.Time) {
		nowLock.Lock()
		now = t
		nowLock.Unlock()
	}

	opts := testDatabaseOptions()
	opts = opts.
		SetInstrumentOptions(
			opts.InstrumentOptions().
				SetMetricsScope(scope).
				SetReportInterval(100 * time.Millisecond)).
		SetClockOptions(
			opts.ClockOptions().SetNowFn(nowFn))

	earliestFlush := retention.FlushTimeStart(defaultTestRetentionOpts, now)
	beforeEarliestFlush := earliestFlush.Add(-defaultTestRetentionOpts.BlockSize())

	sleepPerSeries := time.Microsecond

	shard := testDatabaseShard(t, opts)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(true).
		SetTickPerSeriesSleepDuration(sleepPerSeries).
		SetTickSeriesBatchSize(1))
	defer shard.Close()

	// Also check that it expires flush states by time
	shard.flushState.statesByTime[xtime.ToUnixNano(earliestFlush)] = fileOpState{
		Status: fileOpSuccess,
	}
	shard.flushState.statesByTime[xtime.ToUnixNano(beforeEarliestFlush)] = fileOpState{
		Status: fileOpSuccess,
	}
	assert.Equal(t, 2, len(shard.flushState.statesByTime))

	var slept time.Duration
	shard.sleepFn = func(t time.Duration) {
		slept += t
		setNow(nowFn().Add(t))
	}

	ctx := context.NewContext()
	defer ctx.Close()

	shard.Write(ctx, ident.StringID("foo"), nowFn(), 1.0, xtime.Second, nil)
	shard.Write(ctx, ident.StringID("bar"), nowFn(), 2.0, xtime.Second, nil)
	shard.Write(ctx, ident.StringID("baz"), nowFn(), 3.0, xtime.Second, nil)

	for {
		counter, ok := testReporter.Counters()["dbshard.insert-queue.inserts"]
		if ok && counter == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	r, err := shard.Tick(context.NewNoOpCanncellable(), nowFn())
	require.NoError(t, err)
	require.Equal(t, 3, r.activeSeries)
	require.Equal(t, 0, r.expiredSeries)
	require.Equal(t, 2*sleepPerSeries, slept) // Never sleeps on the first series

	// Ensure flush states by time was expired correctly
	require.Equal(t, 1, len(shard.flushState.statesByTime))
	_, ok := shard.flushState.statesByTime[xtime.ToUnixNano(earliestFlush)]
	require.True(t, ok)
}

// This tests a race in shard ticking with an empty series pending expiration.
func TestShardTickRace(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()

	addTestSeries(shard, ident.StringID("foo"))
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		shard.Tick(context.NewNoOpCanncellable(), time.Now())
		wg.Done()
	}()

	go func() {
		shard.Tick(context.NewNoOpCanncellable(), time.Now())
		wg.Done()
	}()

	wg.Wait()

	shard.RLock()
	shardlen := shard.lookup.Len()
	shard.RUnlock()
	require.Equal(t, 0, shardlen)
}

// Catches a logic bug we had trying to purgeSeries and counted the reference
// we had while trying to purge as a concurrent read.
func TestShardTickCleanupSmallBatchSize(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	addTestSeries(shard, ident.StringID("foo"))
	shard.Tick(context.NewNoOpCanncellable(), time.Now())
	require.Equal(t, 0, shard.lookup.Len())
}

// This tests ensures the shard returns an error if two ticks are triggered concurrently.
func TestShardReturnsErrorForConcurrentTicks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	shard.currRuntimeOptions.tickSleepSeriesBatchSize = 1
	shard.currRuntimeOptions.tickSleepPerSeries = time.Millisecond

	var (
		foo     = addMockTestSeries(ctrl, shard, ident.StringID("foo"))
		tick1Wg sync.WaitGroup
		tick2Wg sync.WaitGroup
		closeWg sync.WaitGroup
	)

	tick1Wg.Add(1)
	tick2Wg.Add(1)
	closeWg.Add(2)

	// wait to return the other tick has returned error
	foo.EXPECT().Tick().Do(func() {
		tick1Wg.Done()
		tick2Wg.Wait()
	}).Return(series.TickResult{}, nil)

	go func() {
		_, err := shard.Tick(context.NewNoOpCanncellable(), time.Now())
		require.NoError(t, err)
		closeWg.Done()
	}()

	go func() {
		tick1Wg.Wait()
		_, err := shard.Tick(context.NewNoOpCanncellable(), time.Now())
		require.Error(t, err)
		tick2Wg.Done()
		closeWg.Done()
	}()

	closeWg.Wait()
}

// This tests ensures the resources held in series contained in the shard are released
// when closing the shard.
func TestShardTicksWhenClosed(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	s := addMockTestSeries(ctrl, shard, ident.StringID("foo"))

	gomock.InOrder(
		s.EXPECT().IsEmpty().Return(true),
		s.EXPECT().Close(),
	)
	require.NoError(t, shard.Close())
}

// This tests ensures the shard terminates Ticks when closing.
func TestShardTicksStopWhenClosing(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	shard.currRuntimeOptions.tickSleepSeriesBatchSize = 1
	shard.currRuntimeOptions.tickSleepPerSeries = time.Millisecond

	var (
		foo     = addMockTestSeries(ctrl, shard, ident.StringID("foo"))
		bar     = addMockTestSeries(ctrl, shard, ident.StringID("bar"))
		closeWg sync.WaitGroup
		orderWg sync.WaitGroup
	)

	orderWg.Add(1)
	gomock.InOrder(
		// loop until the shard is marked for Closing
		foo.EXPECT().Tick().Do(func() {
			orderWg.Done()
			for {
				if shard.isClosing() {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}).Return(series.TickResult{}, nil),
		// for the shard Close purging
		foo.EXPECT().IsEmpty().Return(true),
		foo.EXPECT().Close(),
		bar.EXPECT().IsEmpty().Return(true),
		bar.EXPECT().Close(),
	)

	closeWg.Add(2)
	go func() {
		shard.Tick(context.NewNoOpCanncellable(), time.Now())
		closeWg.Done()
	}()

	go func() {
		orderWg.Wait()
		require.NoError(t, shard.Close())
		closeWg.Done()
	}()

	closeWg.Wait()
}

// This tests the scenario where an empty series is expired.
func TestPurgeExpiredSeriesEmptySeries(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()

	addTestSeries(shard, ident.StringID("foo"))

	shard.Tick(context.NewNoOpCanncellable(), time.Now())

	shard.RLock()
	require.Equal(t, 0, shard.lookup.Len())
	shard.RUnlock()
}

// This tests the scenario where a non-empty series is not expired.
func TestPurgeExpiredSeriesNonEmptySeries(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	ctx := opts.ContextPool().Get()
	nowFn := opts.ClockOptions().NowFn()
	shard.Write(ctx, ident.StringID("foo"), nowFn(), 1.0, xtime.Second, nil)
	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular)
	require.NoError(t, err)
	require.Equal(t, 1, r.activeSeries)
	require.Equal(t, 0, r.expiredSeries)
}

// This tests the scenario where a series is empty when series.Tick() is called,
// but receives writes after tickForEachSeries finishes but before purgeExpiredSeries
// starts. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterTicking(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ident.StringID("foo")
	s := addMockSeries(ctrl, shard, id, ident.Tags{}, 0)
	s.EXPECT().Tick().Do(func() {
		// Emulate a write taking place just after tick for this series
		s.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		ctx := opts.ContextPool().Get()
		nowFn := opts.ClockOptions().NowFn()
		shard.Write(ctx, id, nowFn(), 1.0, xtime.Second, nil)
	}).Return(series.TickResult{}, series.ErrSeriesAllDatapointsExpired)

	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular)
	require.NoError(t, err)
	require.Equal(t, 0, r.activeSeries)
	require.Equal(t, 1, r.expiredSeries)
	require.Equal(t, 1, shard.lookup.Len())
}

// This tests the scenario where tickForEachSeries finishes, and before purgeExpiredSeries
// starts, we receive a write for a series, then purgeExpiredSeries runs, then we write to
// the series. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterPurging(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	var entry *lookup.Entry

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ident.StringID("foo")
	s := addMockSeries(ctrl, shard, id, ident.Tags{}, 0)
	s.EXPECT().Tick().Do(func() {
		// Emulate a write taking place and staying open just after tick for this series
		var err error
		entry, err = shard.writableSeries(id, ident.EmptyTagIterator)
		require.NoError(t, err)
	}).Return(series.TickResult{}, series.ErrSeriesAllDatapointsExpired)

	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular)
	require.NoError(t, err)
	require.Equal(t, 0, r.activeSeries)
	require.Equal(t, 1, r.expiredSeries)
	require.Equal(t, 1, shard.lookup.Len())

	entry.DecrementReaderWriterCount()
	require.Equal(t, 1, shard.lookup.Len())
}

func TestForEachShardEntry(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	for i := 0; i < 10; i++ {
		addTestSeries(shard, ident.StringID(fmt.Sprintf("foo.%d", i)))
	}

	count := 0
	entryFn := func(entry *lookup.Entry) bool {
		if entry.Series.ID().String() == "foo.8" {
			return false
		}

		// Ensure the readerwriter count is incremented while we operate
		// on this series
		assert.Equal(t, int32(1), entry.ReaderWriterCount())

		count++
		return true
	}

	shard.forEachShardEntry(entryFn)

	assert.Equal(t, 8, count)

	// Ensure that reader writer count gets reset
	shard.RLock()
	for elem := shard.list.Front(); elem != nil; elem = elem.Next() {
		entry := elem.Value.(*lookup.Entry)
		assert.Equal(t, int32(0), entry.ReaderWriterCount())
	}
	shard.RUnlock()
}

func TestShardFetchBlocksIDNotExists(t *testing.T) {
	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	fetched, err := shard.FetchBlocks(ctx, ident.StringID("foo"), nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(fetched))
}

func TestShardFetchBlocksIDExists(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ident.StringID("foo")
	series := addMockSeries(ctrl, shard, id, ident.Tags{}, 0)
	now := time.Now()
	starts := []time.Time{now}
	expected := []block.FetchBlockResult{block.NewFetchBlockResult(now, nil, nil)}
	series.EXPECT().FetchBlocks(ctx, starts).Return(expected, nil)
	res, err := shard.FetchBlocks(ctx, id, starts)
	require.NoError(t, err)
	require.Equal(t, expected, res)
}

func TestShardCleanupExpiredFileSets(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	shard.filesetBeforeFn = func(_ string, namespace ident.ID, shardID uint32, t time.Time) ([]string, error) {
		return []string{namespace.String(), strconv.Itoa(int(shardID))}, nil
	}
	var deletedFiles []string
	shard.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}
	require.NoError(t, shard.CleanupExpiredFileSets(time.Now()))
	require.Equal(t, []string{defaultTestNs1ID.String(), "0"}, deletedFiles)
}

func TestShardCleanupSnapshot(t *testing.T) {
	var (
		opts                = testDatabaseOptions()
		shard               = testDatabaseShard(t, opts)
		blockSize           = 2 * time.Hour
		now                 = time.Now().Truncate(blockSize)
		earliestToRetain    = now.Add(-4 * blockSize)
		pastRetention       = earliestToRetain.Add(-blockSize)
		successfullyFlushed = earliestToRetain
		notFlushedYet       = earliestToRetain.Add(blockSize)
	)

	shard.markFlushStateSuccess(earliestToRetain)
	defer shard.Close()

	shard.snapshotFilesFn = func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
		return fs.FileSetFilesSlice{
			// Should get removed for not being in retention period
			fs.FileSetFile{
				ID: fs.FileSetFileIdentifier{
					Namespace:   namespace,
					Shard:       shard,
					BlockStart:  pastRetention,
					VolumeIndex: 0,
				},
				AbsoluteFilepaths: []string{"not-in-retention"},
			},
			// Should get removed for being flushed
			fs.FileSetFile{
				ID: fs.FileSetFileIdentifier{
					Namespace:   namespace,
					Shard:       shard,
					BlockStart:  successfullyFlushed,
					VolumeIndex: 0,
				},
				AbsoluteFilepaths: []string{"successfully-flushed"},
			},
			// Should not get removed - Note that this entry precedes the
			// next in order to ensure that the sorting logic works correctly.
			fs.FileSetFile{
				ID: fs.FileSetFileIdentifier{
					Namespace:   namespace,
					Shard:       shard,
					BlockStart:  notFlushedYet,
					VolumeIndex: 1,
				},
				// Note this filename needs to contain the word "checkpoint" to
				// pass the HasCheckpointFile() check
				AbsoluteFilepaths: []string{"latest-index-and-has-checkpoint"},
			},
			// Should get removed because the next one has a higher index
			fs.FileSetFile{
				ID: fs.FileSetFileIdentifier{
					Namespace:   namespace,
					Shard:       shard,
					BlockStart:  notFlushedYet,
					VolumeIndex: 0,
				},
				AbsoluteFilepaths: []string{"not-latest-index"},
			},
		}, nil
	}

	deletedFiles := []string{}
	shard.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}
	require.NoError(t, shard.CleanupSnapshots(earliestToRetain))

	expectedDeletedFiles := []string{
		"not-in-retention", "successfully-flushed", "not-latest-index"}
	require.Equal(t, expectedDeletedFiles, deletedFiles)
}

type testCloser struct {
	called int
}

func (c *testCloser) Close() {
	c.called++
}

func TestShardRegisterRuntimeOptionsListeners(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	callRegisterListenerOnShard := 0
	callRegisterListenerOnShardInsertQueue := 0

	closer := &testCloser{}

	runtimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	runtimeOptsMgr.EXPECT().
		RegisterListener(gomock.Any()).
		Times(2).
		Do(func(l runtime.OptionsListener) {
			if _, ok := l.(*dbShard); ok {
				callRegisterListenerOnShard++
			}
			if _, ok := l.(*dbShardInsertQueue); ok {
				callRegisterListenerOnShardInsertQueue++
			}
		}).
		Return(closer)

	opts := testDatabaseOptions().
		SetRuntimeOptionsManager(runtimeOptsMgr)

	shard := testDatabaseShard(t, opts)

	assert.Equal(t, 1, callRegisterListenerOnShard)
	assert.Equal(t, 1, callRegisterListenerOnShardInsertQueue)

	assert.Equal(t, 0, closer.called)

	shard.Close()

	assert.Equal(t, 2, closer.called)
}

func TestShardReadEncodedCachesSeriesWithRecentlyReadPolicy(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions().SetSeriesCachePolicy(series.CacheRecentlyRead)
	shard := testDatabaseShard(t, opts)
	defer shard.Close()

	ropts := shard.seriesOpts.RetentionOptions()
	end := opts.ClockOptions().NowFn()().Truncate(ropts.BlockSize())
	start := end.Add(-2 * ropts.BlockSize())
	shard.markFlushStateSuccess(start)
	shard.markFlushStateSuccess(start.Add(ropts.BlockSize()))

	retriever := block.NewMockDatabaseBlockRetriever(ctrl)
	shard.setBlockRetriever(retriever)

	segments := []ts.Segment{
		ts.NewSegment(checked.NewBytes([]byte("bar"), nil), nil, ts.FinalizeNone),
		ts.NewSegment(checked.NewBytes([]byte("baz"), nil), nil, ts.FinalizeNone),
	}

	var blockReaders []xio.BlockReader
	for range segments {
		reader := xio.NewMockSegmentReader(ctrl)
		block := xio.BlockReader{
			SegmentReader: reader,
		}

		blockReaders = append(blockReaders, block)
	}

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	mid := start.Add(ropts.BlockSize())

	retriever.EXPECT().
		Stream(ctx, shard.shard, ident.NewIDMatcher("foo"),
			start, shard.seriesOnRetrieveBlock).
		Do(func(ctx context.Context, shard uint32, id ident.ID, at time.Time, onRetrieve block.OnRetrieveBlock) {
			go onRetrieve.OnRetrieveBlock(id, ident.EmptyTagIterator, at, segments[0])
		}).
		Return(blockReaders[0], nil)
	retriever.EXPECT().
		Stream(ctx, shard.shard, ident.NewIDMatcher("foo"),
			mid, shard.seriesOnRetrieveBlock).
		Do(func(ctx context.Context, shard uint32, id ident.ID, at time.Time, onRetrieve block.OnRetrieveBlock) {
			go onRetrieve.OnRetrieveBlock(id, ident.EmptyTagIterator, at, segments[1])
		}).
		Return(blockReaders[1], nil)

	// Check reads as expected
	r, err := shard.ReadEncoded(ctx, ident.StringID("foo"), start, end)
	require.NoError(t, err)
	require.Equal(t, 2, len(r))
	for i, readers := range r {
		require.Equal(t, 1, len(readers))
		assert.Equal(t, blockReaders[i], readers[0])
	}

	// Check that gets cached
	begin := time.Now()
	for time.Since(begin) < 10*time.Second {
		shard.RLock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID("foo"))
		shard.RUnlock()
		if err == errShardEntryNotFound {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		if err != nil || entry.Series.NumActiveBlocks() == 2 {
			// Expecting at least 2 active blocks and never an error
			break
		}
	}

	shard.RLock()
	entry, _, err := shard.lookupEntryWithLock(ident.StringID("foo"))
	shard.RUnlock()
	require.NoError(t, err)
	require.NotNil(t, entry)

	assert.False(t, entry.Series.IsEmpty())
	assert.Equal(t, 2, entry.Series.NumActiveBlocks())
}

func TestShardNewInvalidShardEntry(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	shard := testDatabaseShard(t, testDatabaseOptions())
	defer shard.Close()

	iter := ident.NewMockTagIterator(ctrl)
	gomock.InOrder(
		iter.EXPECT().Duplicate().Return(iter),
		iter.EXPECT().CurrentIndex().Return(0),
		iter.EXPECT().Next().Return(false),
		iter.EXPECT().Err().Return(fmt.Errorf("random err")),
		iter.EXPECT().Close(),
	)

	_, err := shard.newShardEntry(ident.StringID("abc"), newTagsIterArg(iter))
	require.Error(t, err)
}

func TestShardNewValidShardEntry(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	shard := testDatabaseShard(t, testDatabaseOptions())
	defer shard.Close()

	_, err := shard.newShardEntry(ident.StringID("abc"), newTagsIterArg(ident.EmptyTagIterator))
	require.NoError(t, err)
}

// TestShardNewEntryDoesNotAlterTags tests that the ID and Tags passed
// to newShardEntry is not altered. There are multiple callers that
// reuse the tag iterator passed all the way through to newShardEntry
// either to retry inserting a series or to finalize the tags at the
// end of a request/response cycle or from a disk retrieve cycle.
func TestShardNewEntryDoesNotAlterIDOrTags(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	shard := testDatabaseShard(t, testDatabaseOptions())
	defer shard.Close()

	seriesID := ident.StringID("foo+bar=baz")
	seriesTags := ident.NewTags(ident.Tag{
		Name:  ident.StringID("bar"),
		Value: ident.StringID("baz"),
	})

	// Ensure copied with call to bytes but no close call, etc
	id := ident.NewMockID(ctrl)
	id.EXPECT().IsNoFinalize().Times(1).Return(false)
	id.EXPECT().Bytes().Times(1).Return(seriesID.Bytes())

	iter := ident.NewMockTagIterator(ctrl)

	// Ensure duplicate called but no close, etc
	iter.EXPECT().
		Duplicate().
		Times(1).
		Return(ident.NewTagsIterator(seriesTags))

	entry, err := shard.newShardEntry(id, newTagsIterArg(iter))
	require.NoError(t, err)

	shard.Lock()
	shard.insertNewShardEntryWithLock(entry)
	shard.Unlock()

	entry, _, err = shard.tryRetrieveWritableSeries(seriesID)
	require.NoError(t, err)

	entryIDBytes := entry.Series.ID().Bytes()
	seriesIDBytes := seriesID.Bytes()

	// Ensure ID equal and not same ref
	assert.True(t, entry.Series.ID().Equal(seriesID))
	// NB(r): Use &slice[0] to get a pointer to the very first byte, i.e. data section
	assert.False(t, unsafe.Pointer(&entryIDBytes[0]) == unsafe.Pointer(&seriesIDBytes[0]))

	// Ensure Tags equal and NOT same ref for tags
	assert.True(t, entry.Series.Tags().Equal(seriesTags))
	require.Equal(t, 1, len(entry.Series.Tags().Values()))

	entryTagNameBytes := entry.Series.Tags().Values()[0].Name.Bytes()
	entryTagValueBytes := entry.Series.Tags().Values()[0].Value.Bytes()
	seriesTagNameBytes := seriesTags.Values()[0].Name.Bytes()
	seriesTagValueBytes := seriesTags.Values()[0].Value.Bytes()

	// NB(r): Use &slice[0] to get a pointer to the very first byte, i.e. data section
	assert.False(t, unsafe.Pointer(&entryTagNameBytes[0]) == unsafe.Pointer(&seriesTagNameBytes[0]))
	assert.False(t, unsafe.Pointer(&entryTagValueBytes[0]) == unsafe.Pointer(&seriesTagValueBytes[0]))
}

// TestShardNewEntryTakesRefToNoFinalizeID ensures that when an ID is
// marked as NoFinalize that newShardEntry simply takes a ref as it can
// safely be assured the ID is not pooled.
func TestShardNewEntryTakesRefToNoFinalizeID(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	shard := testDatabaseShard(t, testDatabaseOptions())
	defer shard.Close()

	seriesID := ident.BytesID([]byte("foo+bar=baz"))
	seriesTags := ident.NewTags(ident.Tag{
		Name:  ident.StringID("bar"),
		Value: ident.StringID("baz"),
	})

	// Ensure copied with call to bytes but no close call, etc
	id := ident.NewMockID(ctrl)
	id.EXPECT().IsNoFinalize().Times(1).Return(true)
	id.EXPECT().Bytes().Times(1).Return(seriesID.Bytes())

	iter := ident.NewMockTagIterator(ctrl)

	// Ensure duplicate called but no close, etc
	iter.EXPECT().
		Duplicate().
		Times(1).
		Return(ident.NewTagsIterator(seriesTags))

	entry, err := shard.newShardEntry(id, newTagsIterArg(iter))
	require.NoError(t, err)

	shard.Lock()
	shard.insertNewShardEntryWithLock(entry)
	shard.Unlock()

	entry, _, err = shard.tryRetrieveWritableSeries(seriesID)
	require.NoError(t, err)

	assert.True(t, entry.Series.ID().Equal(seriesID))

	entryIDBytes := entry.Series.ID().Bytes()
	seriesIDBytes := seriesID.Bytes()

	// Ensure ID equal and same ref
	assert.True(t, entry.Series.ID().Equal(seriesID))
	// NB(r): Use &slice[0] to get a pointer to the very first byte, i.e. data section
	assert.True(t, unsafe.Pointer(&entryIDBytes[0]) == unsafe.Pointer(&seriesIDBytes[0]))

	// Ensure Tags equal and NOT same ref for tags
	assert.True(t, entry.Series.Tags().Equal(seriesTags))
	require.Equal(t, 1, len(entry.Series.Tags().Values()))

	entryTagNameBytes := entry.Series.Tags().Values()[0].Name.Bytes()
	entryTagValueBytes := entry.Series.Tags().Values()[0].Value.Bytes()
	seriesTagNameBytes := seriesTags.Values()[0].Name.Bytes()
	seriesTagValueBytes := seriesTags.Values()[0].Value.Bytes()

	// NB(r): Use &slice[0] to get a pointer to the very first byte, i.e. data section
	assert.False(t, unsafe.Pointer(&entryTagNameBytes[0]) == unsafe.Pointer(&seriesTagNameBytes[0]))
	assert.False(t, unsafe.Pointer(&entryTagValueBytes[0]) == unsafe.Pointer(&seriesTagValueBytes[0]))
}

func TestShardIterateBatchSize(t *testing.T) {
	smaller := shardIterateBatchMinSize - 1
	require.Equal(t, smaller, iterateBatchSize(smaller))

	require.Equal(t, shardIterateBatchMinSize, iterateBatchSize(shardIterateBatchMinSize+1))

	require.True(t, shardIterateBatchMinSize < iterateBatchSize(2000))
}
