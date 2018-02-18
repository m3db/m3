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

	"github.com/fortytw2/leaktest"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/storage/series"
	"github.com/m3db/m3db/ts"
	xmetrics "github.com/m3db/m3db/x/metrics"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
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
	_, s := testDatabaseShardWithIndexFn(t, opts, databaseIndexNoOpWriteFn)
	return s
}

func testDatabaseShardWithIndexFn(
	t *testing.T,
	opts Options,
	fn databaseIndexWriteFn,
) (*dbNamespace, *dbShard) {
	ns := newTestNamespace(t)
	nsReaderMgr := newNamespaceReaderManager(ns.metadata, tally.NoopScope, opts)
	seriesOpts := NewSeriesOptionsFromOptions(opts, ns.Options().RetentionOptions())
	return ns, newDatabaseShard(ns.metadata, 0, nil, nsReaderMgr,
		&testIncreasingIndex{}, commitLogWriteNoOp, fn, true, opts, seriesOpts).(*dbShard)
}

func addMockSeries(ctrl *gomock.Controller, shard *dbShard, id ident.ID, tags ident.Tags, index uint64) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().Return(id).AnyTimes()
	series.EXPECT().Tags().Return(tags).AnyTimes()
	series.EXPECT().IsEmpty().Return(false).AnyTimes()
	shard.lookup[id.Hash()] = shard.list.PushBack(&dbShardEntry{series: series, index: index})
	return series
}

func TestShardDontNeedBootstrap(t *testing.T) {
	opts := testDatabaseOptions()
	testNs := newTestNamespace(t)
	seriesOpts := NewSeriesOptionsFromOptions(opts, testNs.Options().RetentionOptions())
	shard := newDatabaseShard(testNs.metadata, 0, nil, nil,
		&testIncreasingIndex{}, commitLogWriteNoOp, databaseIndexNoOpWriteFn,
		false, opts, seriesOpts).(*dbShard)
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
	fooSeries.EXPECT().ID().Return(ident.StringID("foo")).AnyTimes()
	fooSeries.EXPECT().IsEmpty().Return(false).AnyTimes()
	barSeries := series.NewMockDatabaseSeries(ctrl)
	barSeries.EXPECT().ID().Return(ident.StringID("bar")).AnyTimes()
	barSeries.EXPECT().IsEmpty().Return(false).AnyTimes()
	s.lookup[ident.StringID("foo").Hash()] = s.list.PushBack(&dbShardEntry{series: fooSeries})
	s.lookup[ident.StringID("bar").Hash()] = s.list.PushBack(&dbShardEntry{series: barSeries})

	fooBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	barBlocks := block.NewMockDatabaseSeriesBlocks(ctrl)
	fooSeries.EXPECT().Bootstrap(fooBlocks).Return(nil)
	fooSeries.EXPECT().IsBootstrapped().Return(true)
	barSeries.EXPECT().Bootstrap(barBlocks).Return(errors.New("series error"))
	barSeries.EXPECT().IsBootstrapped().Return(true)

	fooID := ident.StringID("foo")
	barID := ident.StringID("bar")

	bootstrappedSeries := map[ident.Hash]result.DatabaseSeriesBlocks{
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
		Persist: func(ident.ID, ts.Segment, uint32) error { return nil },
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
		series.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
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
		Persist: func(ident.ID, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}

	flush.EXPECT().Prepare(namespace.NewMetadataMatcher(s.namespace),
		s.shard, blockStart).Return(prepared, nil)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		series := series.NewMockDatabaseSeries(ctrl)
		series.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
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

func addMockTestSeries(ctrl *gomock.Controller, shard *dbShard, id ident.ID) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().AnyTimes().Return(id)
	shard.Lock()
	shard.lookup[id.Hash()] = shard.list.PushBack(&dbShardEntry{series: series})
	shard.Unlock()
	return series
}

func addTestSeries(shard *dbShard, id ident.ID) series.DatabaseSeries {
	series := series.NewDatabaseSeries(id, nil, shard.seriesOpts)
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

	shard.Write(ctx, ident.StringID("foo"), nowFn(), 1.0, xtime.Second, nil)
	shard.Write(ctx, ident.StringID("bar"), nowFn(), 2.0, xtime.Second, nil)
	shard.Write(ctx, ident.StringID("baz"), nowFn(), 3.0, xtime.Second, nil)

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

	addTestSeries(shard, ident.StringID("foo"))
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
	s := addMockTestSeries(ctrl, shard, ident.StringID("foo"))

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

	addTestSeries(shard, ident.StringID("foo"))

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ident.StringID("foo")
	s := addMockSeries(ctrl, shard, id, nil, 0)
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
	id := ident.StringID("foo")
	s := addMockSeries(ctrl, shard, id, nil, 0)
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
	require.Equal(t, 1, len(shard.lookup))

	entry.decrementReaderWriterCount()
	require.Equal(t, 1, len(shard.lookup))
}

func TestForEachShardEntry(t *testing.T) {
	opts := testDatabaseOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	for i := 0; i < 10; i++ {
		addTestSeries(shard, ident.StringID(fmt.Sprintf("foo.%d", i)))
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
	fetched, err := shard.FetchBlocks(ctx, ident.StringID("foo"), nil)
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
	id := ident.StringID("foo")
	series := addMockSeries(ctrl, shard, id, nil, 0)
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
	shard.filesetBeforeFn = func(_ string, namespace ident.ID, shardID uint32, t time.Time) ([]string, error) {
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

func TestShardReadEncodedCachesSeriesWithRecentlyReadPolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
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

	var segReaders []*xio.MockSegmentReader
	for range segments {
		reader := xio.NewMockSegmentReader(ctrl)
		segReaders = append(segReaders, reader)
	}

	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	retriever.EXPECT().
		Stream(ctx, shard.shard, ident.NewIDMatcher("foo"),
			start, shard.seriesOnRetrieveBlock).
		Do(func(ctx context.Context, shard uint32, id ident.ID, at time.Time, onRetrieve block.OnRetrieveBlock) {
			go onRetrieve.OnRetrieveBlock(id, at, segments[0])
		}).
		Return(segReaders[0], nil)
	retriever.EXPECT().
		Stream(ctx, shard.shard, ident.NewIDMatcher("foo"),
			start.Add(ropts.BlockSize()), shard.seriesOnRetrieveBlock).
		Do(func(ctx context.Context, shard uint32, id ident.ID, at time.Time, onRetrieve block.OnRetrieveBlock) {
			go onRetrieve.OnRetrieveBlock(id, at, segments[1])
		}).
		Return(segReaders[1], nil)

	// Check reads as expected
	r, err := shard.ReadEncoded(ctx, ident.StringID("foo"), start, end)
	require.NoError(t, err)
	require.Equal(t, 2, len(r))
	for i, readers := range r {
		require.Equal(t, 1, len(readers))
		assert.Equal(t, segReaders[i], readers[0])
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

		if err != nil || entry.series.NumActiveBlocks() == 2 {
			// Expecting at least 2 active blocks and never an error
			break
		}
	}

	shard.RLock()
	entry, _, err := shard.lookupEntryWithLock(ident.StringID("foo"))
	shard.RUnlock()
	require.NoError(t, err)
	require.NotNil(t, entry)

	assert.False(t, entry.series.IsEmpty())
	assert.Equal(t, 2, entry.series.NumActiveBlocks())
}

func TestShardInsertDatabaseIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()
	opts := testDatabaseOptions().SetIndexingEnabled(true)

	lock := sync.Mutex{}
	indexWrites := []testIndexWrite{}

	mockWriteFn := func(ns ident.ID, id ident.ID, tags ident.Tags) error {
		lock.Lock()
		indexWrites = append(indexWrites, testIndexWrite{id: id, ns: ns, tags: tags})
		lock.Unlock()
		return nil
	}

	dbNs, shard := testDatabaseShardWithIndexFn(t, opts, mockWriteFn)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(false))
	defer shard.Close()
	defer dbNs.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 1.0, xtime.Second, nil))

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 2.0, xtime.Second, nil))

	require.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), time.Now(), 1.0, xtime.Second, nil))

	lock.Lock()
	defer lock.Unlock()

	require.Len(t, indexWrites, 1)
	require.Equal(t, "foo", indexWrites[0].id.String())
	require.Equal(t, "testns1", indexWrites[0].ns.String())
	require.Equal(t, "name", indexWrites[0].tags[0].Name.String())
	require.Equal(t, "value", indexWrites[0].tags[0].Value.String())
}

func TestShardAsyncInsertDatabaseIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	opts := testDatabaseOptions().SetIndexingEnabled(true)
	lock := sync.RWMutex{}
	indexWrites := []testIndexWrite{}

	mockWriteFn := func(ns ident.ID, id ident.ID, tags ident.Tags) error {
		lock.Lock()
		indexWrites = append(indexWrites, testIndexWrite{id: id, ns: ns, tags: tags})
		lock.Unlock()
		return nil
	}

	dbNs, shard := testDatabaseShardWithIndexFn(t, opts, mockWriteFn)
	shard.SetRuntimeOptions(runtime.NewOptions().SetWriteNewSeriesAsync(true))
	defer shard.Close()
	defer dbNs.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 1.0, xtime.Second, nil))

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"),
			ident.NewTagIterator(ident.StringTag("name", "value")),
			time.Now(), 2.0, xtime.Second, nil))

	require.NoError(t,
		shard.Write(ctx, ident.StringID("bar"), time.Now(), 1.0, xtime.Second, nil))

	require.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("baz"),
			ident.NewTagIterator(
				ident.StringTag("all", "tags"),
				ident.StringTag("should", "be-present"),
			),
			time.Now(), 1.0, xtime.Second, nil))

	for {
		lock.RLock()
		l := len(indexWrites)
		lock.RUnlock()
		if l == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	lock.Lock()
	defer lock.Unlock()

	require.Len(t, indexWrites, 2)
	for _, w := range indexWrites {
		if w.id.String() == "foo" {
			require.Equal(t, "testns1", w.ns.String())
			require.Len(t, w.tags, 1)
			require.Equal(t, "name", w.tags[0].Name.String())
			require.Equal(t, "value", w.tags[0].Value.String())
		} else if w.id.String() == "baz" {
			require.Equal(t, "testns1", w.ns.String())
			require.Len(t, w.tags, 2)
			require.Equal(t, "all", w.tags[0].Name.String())
			require.Equal(t, "tags", w.tags[0].Value.String())
			require.Equal(t, "should", w.tags[1].Name.String())
			require.Equal(t, "be-present", w.tags[1].Value.String())
		} else {
			require.Fail(t, "unexpected write", w)
		}
	}
}

type testIndexWrite struct {
	id   ident.ID
	ns   ident.ID
	tags ident.Tags
}
