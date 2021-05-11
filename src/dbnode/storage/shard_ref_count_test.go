// Copyright (c) 2018 Uber Technologies, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/series"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestShardWriteSyncRefCount(t *testing.T) {
	opts := DefaultTestOptions()
	testShardWriteSyncRefCount(t, opts)
}

func TestShardWriteSyncRefCountVerifyNoCopyAnnotation(t *testing.T) {
	opts := DefaultTestOptions().
		// Set bytes pool to nil to ensure we're not using it to copy annotations
		// on the sync path.
		SetBytesPool(nil)
	testShardWriteSyncRefCount(t, opts)
}

func testShardWriteSyncRefCount(t *testing.T, opts Options) {
	now := time.Now()

	shard := testDatabaseShard(t, opts)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	seriesWrite, err := shard.Write(ctx, ident.StringID("foo"), now, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("foo"), now, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.False(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("bar"), now, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("baz"), now, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}

	// write already inserted series'
	next := now.Add(time.Minute)

	seriesWrite, err = shard.Write(ctx, ident.StringID("foo"), next, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("bar"), next, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("baz"), next, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}

func TestShardWriteTaggedSyncRefCountMockIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := namespaceIndexOptions.BlockSize()

	idx := NewMockNamespaceIndex(ctrl)
	idx.EXPECT().BlockStartForWriteTime(gomock.Any()).
		DoAndReturn(func(t time.Time) xtime.UnixNano {
			return xtime.ToUnixNano(t.Truncate(blockSize))
		}).
		AnyTimes()
	idx.EXPECT().WriteBatch(gomock.Any()).
		Return(nil).
		Do(func(batch *index.WriteBatch) {
			if batch.Len() != 1 {
				// require.Equal(...) silently kills goroutines
				panic(fmt.Sprintf("expected batch len 1: len=%d", batch.Len()))
			}

			entry := batch.PendingEntries()[0]
			blockStart := xtime.ToUnixNano(entry.Timestamp.Truncate(blockSize))
			onIdx := entry.OnIndexSeries
			onIdx.OnIndexSuccess(blockStart)
			onIdx.OnIndexFinalize(blockStart)
		}).
		AnyTimes()

	testShardWriteTaggedSyncRefCount(t, idx)
}

func TestShardWriteTaggedSyncRefCountSyncIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	newFn := func(fn nsIndexInsertBatchFn, md namespace.Metadata, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, md, nowFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	var (
		opts      = DefaultTestOptions()
		indexOpts = opts.IndexOptions().
				SetInsertMode(index.InsertSync)
	)
	opts = opts.SetIndexOptions(indexOpts)

	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, opts)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, idx.Close())
	}()

	testShardWriteTaggedSyncRefCount(t, idx)
}

func testShardWriteTaggedSyncRefCount(t *testing.T, idx NamespaceIndex) {
	var (
		now   = time.Now()
		opts  = DefaultTestOptions()
		shard = testDatabaseShardWithIndexFn(t, opts, idx, false)
	)

	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	seriesWrite, err := shard.WriteTagged(ctx, ident.StringID("foo"),
		convert.EmptyTagMetadataResolver, now, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("bar"),
		convert.EmptyTagMetadataResolver, now, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("baz"),
		convert.EmptyTagMetadataResolver, now, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}

	// write already inserted series'
	next := now.Add(time.Minute)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("foo"),
		convert.EmptyTagMetadataResolver, next, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("bar"),
		convert.EmptyTagMetadataResolver, next, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("baz"),
		convert.EmptyTagMetadataResolver, next, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}

func TestShardWriteAsyncRefCount(t *testing.T) {
	testReporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)
	defer closer.Close()

	now := time.Now()
	opts := DefaultTestOptions()
	opts = opts.SetInstrumentOptions(
		opts.InstrumentOptions().
			SetMetricsScope(scope).
			SetReportInterval(100 * time.Millisecond))

	shard := testDatabaseShard(t, opts)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	seriesWrite, err := shard.Write(ctx, ident.StringID("foo"), now, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("bar"), now, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("baz"), now, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	inserted := clock.WaitUntil(func() bool {
		counter, ok := testReporter.Counters()["dbshard.insert-queue.inserts"]
		return ok && counter == 3
	}, 2*time.Second)
	assert.True(t, inserted)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}

	// write already inserted series'
	next := now.Add(time.Minute)

	seriesWrite, err = shard.Write(ctx, ident.StringID("foo"), next, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("bar"), next, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.Write(ctx, ident.StringID("baz"), next, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}

func newNowFnForWriteTaggedAsyncRefCount() func() time.Time {
	// Explicitly truncate the time to the beginning of the index block size to prevent
	// the ref-counts from being thrown off by needing to re-index entries because the first
	// write happened near a block boundary so that when the second write comes in they need
	// to be re-indexed for the next block time.
	start := time.Now().Truncate(namespaceIndexOptions.BlockSize())

	return func() time.Time {
		return start
	}
}
func TestShardWriteTaggedAsyncRefCountMockIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := namespaceIndexOptions.BlockSize()

	idx := NewMockNamespaceIndex(ctrl)
	idx.EXPECT().BlockStartForWriteTime(gomock.Any()).
		DoAndReturn(func(t time.Time) xtime.UnixNano {
			return xtime.ToUnixNano(t.Truncate(blockSize))
		}).
		AnyTimes()
	idx.EXPECT().WriteBatch(gomock.Any()).
		Return(nil).
		Do(func(batch *index.WriteBatch) {
			for _, entry := range batch.PendingEntries() {
				blockStart := xtime.ToUnixNano(entry.Timestamp.Truncate(blockSize))
				onIdx := entry.OnIndexSeries
				onIdx.OnIndexSuccess(blockStart)
				onIdx.OnIndexFinalize(blockStart)
			}
		}).
		AnyTimes()

	nowFn := newNowFnForWriteTaggedAsyncRefCount()
	testShardWriteTaggedAsyncRefCount(t, idx, nowFn)
}

func TestShardWriteTaggedAsyncRefCountSyncIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	newFn := func(fn nsIndexInsertBatchFn, md namespace.Metadata,
		nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, md, nowFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	nowFn := newNowFnForWriteTaggedAsyncRefCount()
	var (
		opts      = DefaultTestOptions()
		indexOpts = opts.IndexOptions().
				SetInsertMode(index.InsertSync)
	)
	opts = opts.
		SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))
	indexOpts = indexOpts.
		SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))
	opts = opts.SetIndexOptions(indexOpts)

	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, opts)
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, idx.Close())
	}()

	testShardWriteTaggedAsyncRefCount(t, idx, nowFn)
}

func testShardWriteTaggedAsyncRefCount(t *testing.T, idx NamespaceIndex, nowFn func() time.Time) {
	testReporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)
	defer closer.Close()

	var (
		start = nowFn()
		now   = start
		opts  = DefaultTestOptions()
	)
	opts = opts.
		SetInstrumentOptions(
			opts.InstrumentOptions().
				SetMetricsScope(scope).
				SetReportInterval(100 * time.Millisecond))
	opts = opts.
		SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	shard := testDatabaseShardWithIndexFn(t, opts, idx, false)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	seriesWrite, err := shard.WriteTagged(ctx, ident.StringID("foo"),
		convert.EmptyTagMetadataResolver, now, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)
	assert.True(t, seriesWrite.NeedsIndex)
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexSuccess(idx.BlockStartForWriteTime(now))
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexFinalize(idx.BlockStartForWriteTime(now))

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("bar"),
		convert.EmptyTagMetadataResolver, now, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)
	assert.True(t, seriesWrite.NeedsIndex)
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexSuccess(idx.BlockStartForWriteTime(now))
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexFinalize(idx.BlockStartForWriteTime(now))

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("baz"),
		convert.EmptyTagMetadataResolver, now, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)
	assert.True(t, seriesWrite.NeedsIndex)
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexSuccess(idx.BlockStartForWriteTime(now))
	seriesWrite.PendingIndexInsert.Entry.OnIndexSeries.OnIndexFinalize(idx.BlockStartForWriteTime(now))

	inserted := clock.WaitUntil(func() bool {
		counter, ok := testReporter.Counters()["dbshard.insert-queue.inserts"]
		return ok && counter == 3
	}, 5*time.Second)
	assert.True(t, inserted)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}

	// write already inserted series'
	next := now.Add(time.Minute)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("foo"),
		convert.EmptyTagMetadataResolver, next, 1.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("bar"),
		convert.EmptyTagMetadataResolver, next, 2.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	seriesWrite, err = shard.WriteTagged(ctx, ident.StringID("baz"),
		convert.EmptyTagMetadataResolver, next, 3.0, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.True(t, seriesWrite.WasWritten)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}
