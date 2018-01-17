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

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	xmetrics "github.com/m3db/m3db/x/metrics"
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
	ns := newTestNamespace(t)
	nsReaderMgr := newNamespaceReaderManager(ns.metadata, tally.NoopScope, opts)
	seriesOpts := NewSeriesOptionsFromOptions(opts, ns.Options().RetentionOptions())
	return newDatabaseShard(ns.metadata, 0, nil, nsReaderMgr,
		&testIncreasingIndex{}, commitLogWriteNoOp, true, opts, seriesOpts).(*dbShard)
}

func addMockSeries(ctrl *gomock.Controller, shard *dbShard, id ts.ID, index uint64) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().Return(id).AnyTimes()
	series.EXPECT().IsEmpty().Return(false).AnyTimes()
	shard.lookup[id.Hash()] = shard.list.PushBack(&dbShardEntry{series: series, index: index})
	return series
}

func TestShardDontNeedBootstrap(t *testing.T) {
	opts := testDatabaseOptions()
	testNs := newTestNamespace(t)
	seriesOpts := NewSeriesOptionsFromOptions(opts, testNs.Options().RetentionOptions())
	shard := newDatabaseShard(testNs.metadata, 0, nil, nil,
		&testIncreasingIndex{}, commitLogWriteNoOp, false, opts, seriesOpts).(*dbShard)
	defer shard.Close()

	require.Equal(t, bootstrapped, shard.bs)
	require.True(t, shard.newSeriesBootstrapped)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	s := testDatabaseShard(t, opts)
	defer s.Close()

	fooSeries := series.NewMockDatabaseSeries(ctrl)
	fooSeries.EXPECT().ID().Return(ts.StringID("foo")).AnyTimes()
	fooSeries.EXPECT().IsEmpty().Return(false).AnyTimes()
	barSeries := series.NewMockDatabaseSeries(ctrl)
	barSeries.EXPECT().ID().Return(ts.StringID("bar")).AnyTimes()
	barSeries.EXPECT().IsEmpty().Return(false).AnyTimes()
	s.lookup[ts.StringID("foo").Hash()] = s.list.PushBack(&dbShardEntry{series: fooSeries})
	s.lookup[ts.StringID("bar").Hash()] = s.list.PushBack(&dbShardEntry{series: barSeries})

	fooBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	barBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	fooSeries.EXPECT().Bootstrap(fooBlocks).Return(nil)
	fooSeries.EXPECT().IsBootstrapped().Return(true)
	barSeries.EXPECT().Bootstrap(barBlocks).Return(errors.New("series error"))
	barSeries.EXPECT().IsBootstrapped().Return(true)

	fooID := ts.StringID("foo")
	barID := ts.StringID("bar")

	bootstrappedSeries := map[ts.Hash]result.DatabaseSeriesBlocks{
		fooID.Hash(): {ID: fooID, Blocks: fooBlocks},
		barID.Hash(): {ID: barID, Blocks: barBlocks},
	}

	err := s.Bootstrap(bootstrappedSeries)

	require.NotNil(t, err)
	require.Equal(t, "series error", err.Error())
	require.Equal(t, bootstrapped, s.bs)
}

func TestShardFlushDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bs = bootstrapping
	err := s.Flush(time.Now(), nil)
	require.Equal(t, err, errShardNotBootstrappedToFlush)
}

func TestShardFlushNoPersistFuncNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bs = bootstrapped
	blockStart := time.Unix(21600, 0)
	flush := persist.NewMockFlush(ctrl)
	prepared := persist.PreparedPersist{Persist: nil}
	flush.EXPECT().Prepare(namespace.NewMetadataMatcher(s.namespace),
		s.shard, blockStart).Return(prepared, nil)

	err := s.Flush(blockStart, flush)
	require.Nil(t, err)

	flushState := s.FlushState(blockStart)

	// Status should be success as returning nil for prepared.Persist
	// signals that there is already a file set written for the block start
	require.Equal(t, fileOpState{
		Status:      fileOpSuccess,
		NumFailures: 0,
	}, flushState)
}

func TestShardFlushNoPersistFuncWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bs = bootstrapped
	blockStart := time.Unix(21600, 0)
	flush := persist.NewMockFlush(ctrl)
	prepared := persist.PreparedPersist{}
	expectedErr := errors.New("some error")

	flush.EXPECT().Prepare(namespace.NewMetadataMatcher(s.namespace),
		s.shard, blockStart).Return(prepared, expectedErr)

	actualErr := s.Flush(blockStart, flush)
	require.NotNil(t, actualErr)
	require.Equal(t, "some error", actualErr.Error())

	flushState := s.FlushState(blockStart)
	require.Equal(t, fileOpState{
		Status:      fileOpFailed,
		NumFailures: 1,
	}, flushState)
}

func TestShardFlushSeriesFlushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockStart := time.Unix(21600, 0)

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bs = bootstrapped
	s.flushState.statesByTime[xtime.ToUnixNano(blockStart)] = fileOpState{
		Status:      fileOpFailed,
		NumFailures: 1,
	}

	var closed bool
	flush := persist.NewMockFlush(ctrl)
	prepared := persist.PreparedPersist{
		Persist: func(ts.ID, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}
	expectedErr := errors.New("error foo")
	flush.EXPECT().Prepare(namespace.NewMetadataMatcher(s.namespace),
		s.shard, blockStart).Return(prepared, expectedErr)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		var expectedErr error
		if i == 1 {
			expectedErr = errors.New("error bar")
		}
		series := series.NewMockDatabaseSeries(ctrl)
		series.EXPECT().ID().Return(ts.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		series.EXPECT().IsEmpty().Return(false).AnyTimes()
		series.EXPECT().
			Flush(gomock.Any(), blockStart, gomock.Any()).
			Do(func(context.Context, time.Time, persist.Fn) {
				flushed[i] = struct{}{}
			}).
			Return(expectedErr)
		s.list.PushBack(&dbShardEntry{series: series})
	}

	err := s.Flush(blockStart, flush)

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.NotNil(t, err)
	require.Equal(t, "error foo\nerror bar", err.Error())

	flushState := s.FlushState(blockStart)
	require.Equal(t, fileOpState{
		Status:      fileOpFailed,
		NumFailures: 2,
	}, flushState)
}

func TestShardFlushSeriesFlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockStart := time.Unix(21600, 0)

	s := testDatabaseShard(t, testDatabaseOptions())
	defer s.Close()
	s.bs = bootstrapped
	s.flushState.statesByTime[xtime.ToUnixNano(blockStart)] = fileOpState{
		Status:      fileOpFailed,
		NumFailures: 1,
	}

	var closed bool
	flush := persist.NewMockFlush(ctrl)
	prepared := persist.PreparedPersist{
		Persist: func(ts.ID, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}

	flush.EXPECT().Prepare(namespace.NewMetadataMatcher(s.namespace),
		s.shard, blockStart).Return(prepared, nil)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		series := series.NewMockDatabaseSeries(ctrl)
		series.EXPECT().ID().Return(ts.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		series.EXPECT().IsEmpty().Return(false).AnyTimes()
		series.EXPECT().
			Flush(gomock.Any(), blockStart, gomock.Any()).
			Do(func(context.Context, time.Time, persist.Fn) {
				flushed[i] = struct{}{}
			}).
			Return(nil)
		s.list.PushBack(&dbShardEntry{series: series})
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

func addMockTestSeries(ctrl *gomock.Controller, shard *dbShard, id ts.ID) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().AnyTimes().Return(id)
	shard.Lock()
	shard.lookup[id.Hash()] = shard.list.PushBack(&dbShardEntry{series: series})
	shard.Unlock()
	return series
}

func addTestSeries(shard *dbShard, id ts.ID) series.DatabaseSeries {
	series := series.NewDatabaseSeries(id, shard.seriesOpts)
	series.Bootstrap(nil)
	shard.Lock()
	shard.lookup[id.Hash()] = shard.list.PushBack(&dbShardEntry{series: series})
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

	shard.Write(ctx, ts.StringID("foo"), nowFn(), 1.0, xtime.Second, nil)
	shard.Write(ctx, ts.StringID("bar"), nowFn(), 2.0, xtime.Second, nil)
	shard.Write(ctx, ts.StringID("baz"), nowFn(), 3.0, xtime.Second, nil)

	r, err := shard.Tick(context.NewNoOpCanncellable())
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

	shard.Write(ctx, ts.StringID("foo"), nowFn(), 1.0, xtime.Second, nil)
	shard.Write(ctx, ts.StringID("bar"), nowFn(), 2.0, xtime.Second, nil)
	shard.Write(ctx, ts.StringID("baz"), nowFn(), 3.0, xtime.Second, nil)

	for {
		counter, ok := testReporter.Counters()["dbshard.insert-queue.inserts"]
		if ok && counter == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	r, err := shard.Tick(context.NewNoOpCanncellable())
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

	addTestSeries(shard, ts.StringID("foo"))
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		shard.Tick(context.NewNoOpCanncellable())
		wg.Done()
	}()

	go func() {
		shard.Tick(context.NewNoOpCanncellable())
		wg.Done()
	}()

	wg.Wait()

	shard.RLock()
	require.Equal(t, 0, len(shard.lookup))
	shard.RUnlock()
}

// This tests ensures the shard returns an error if two ticks are triggered concurrently.
func TestShardReturnsErrorForConcurrentTicks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	shard.currRuntimeOptions.tickSleepSeriesBatchSize = 1
	shard.currRuntimeOptions.tickSleepPerSeries = time.Millisecond

	var (
		foo     = addMockTestSeries(ctrl, shard, ts.StringID("foo"))
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
		_, err := shard.Tick(context.NewNoOpCanncellable())
		require.NoError(t, err)
		closeWg.Done()
	}()

	go func() {
		tick1Wg.Wait()
		_, err := shard.Tick(context.NewNoOpCanncellable())
		require.Error(t, err)
		tick2Wg.Done()
		closeWg.Done()
	}()

	closeWg.Wait()
}

// This tests ensures the resources held in series contained in the shard are released
// when closing the shard.
func TestShardTicksWhenClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	s := addMockTestSeries(ctrl, shard, ts.StringID("foo"))

	gomock.InOrder(
		s.EXPECT().IsEmpty().Return(true),
		s.EXPECT().Close(),
	)
	require.NoError(t, shard.Close())
}

// This tests ensures the shard terminates Ticks when closing.
func TestShardTicksStopWhenClosing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	shard.currRuntimeOptions.tickSleepSeriesBatchSize = 1
	shard.currRuntimeOptions.tickSleepPerSeries = time.Millisecond

	var (
		foo     = addMockTestSeries(ctrl, shard, ts.StringID("foo"))
		bar     = addMockTestSeries(ctrl, shard, ts.StringID("bar"))
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
		shard.Tick(context.NewNoOpCanncellable())
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

	addTestSeries(shard, ts.StringID("foo"))

	shard.Tick(context.NewNoOpCanncellable())

	shard.RLock()
	require.Equal(t, 0, len(shard.lookup))
	shard.RUnlock()
}

// This tests the scenario where a non-empty series is not expired.
func TestPurgeExpiredSeriesNonEmptySeries(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	ctx := opts.ContextPool().Get()
	nowFn := opts.ClockOptions().NowFn()
	shard.Write(ctx, ts.StringID("foo"), nowFn(), 1.0, xtime.Second, nil)
	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular)
	require.NoError(t, err)
	require.Equal(t, 1, r.activeSeries)
	require.Equal(t, 0, r.expiredSeries)
}

// This tests the scenario where a series is empty when series.Tick() is called,
// but receives writes after tickForEachSeries finishes but before purgeExpiredSeries
// starts. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterTicking(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ts.StringID("foo")
	s := addMockSeries(ctrl, shard, id, 0)
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
	require.Equal(t, 1, len(shard.lookup))
}

// This tests the scenario where tickForEachSeries finishes, and before purgeExpiredSeries
// starts, we receive a write for a series, then purgeExpiredSeries runs, then we write to
// the series. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterPurging(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var entry *dbShardEntry

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ts.StringID("foo")
	s := addMockSeries(ctrl, shard, id, 0)
	s.EXPECT().Tick().Do(func() {
		// Emulate a write taking place and staying open just after tick for this series
		var err error
		entry, err = shard.writableSeries(id)
		require.NoError(t, err)
	}).Return(series.TickResult{}, series.ErrSeriesAllDatapointsExpired)

	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular)
	require.NoError(t, err)
	require.Equal(t, 0, r.activeSeries)
	require.Equal(t, 1, r.expiredSeries)
	require.Equal(t, 1, len(shard.lookup))

	entry.decrementReaderWriterCount()
	require.Equal(t, 1, len(shard.lookup))
}

func TestForEachShardEntry(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	for i := 0; i < 10; i++ {
		addTestSeries(shard, ts.StringID(fmt.Sprintf("foo.%d", i)))
	}

	count := 0
	entryFn := func(entry *dbShardEntry) bool {
		if entry.series.ID().String() == "foo.8" {
			return false
		}

		// Ensure the readerwriter count is incremented while we operate
		// on this series
		assert.Equal(t, int32(1), entry.readerWriterCount())

		count++
		return true
	}

	shard.forEachShardEntry(entryFn)

	assert.Equal(t, 8, count)

	// Ensure that reader writer count gets reset
	shard.RLock()
	for elem := shard.list.Front(); elem != nil; elem = elem.Next() {
		entry := elem.Value.(*dbShardEntry)
		assert.Equal(t, int32(0), entry.readerWriterCount())
	}
	shard.RUnlock()
}

func TestShardFetchBlocksIDNotExists(t *testing.T) {
	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	fetched, err := shard.FetchBlocks(ctx, ts.StringID("foo"), nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(fetched))
}

func TestShardFetchBlocksIDExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ts.StringID("foo")
	series := addMockSeries(ctrl, shard, id, 0)
	now := time.Now()
	starts := []time.Time{now}
	expected := []block.FetchBlockResult{block.NewFetchBlockResult(now, nil, nil)}
	series.EXPECT().FetchBlocks(ctx, starts).Return(expected, nil)
	res, err := shard.FetchBlocks(ctx, id, starts)
	require.NoError(t, err)
	require.Equal(t, expected, res)
}

func TestShardCleanupFileset(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	shard.filesetBeforeFn = func(_ string, namespace ts.ID, shardID uint32, t time.Time) ([]string, error) {
		return []string{namespace.String(), strconv.Itoa(int(shardID))}, nil
	}
	var deletedFiles []string
	shard.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}
	require.NoError(t, shard.CleanupFileset(time.Now()))
	require.Equal(t, []string{defaultTestNs1ID.String(), "0"}, deletedFiles)
}

type testCloser struct {
	called int
}

func (c *testCloser) Close() {
	c.called++
}

func TestShardRegisterRuntimeOptionsListeners(t *testing.T) {
	ctrl := gomock.NewController(t)
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
