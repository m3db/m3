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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestShardWriteSyncRefCount(t *testing.T) {
	now := time.Now()
	opts := testDatabaseOptions()

	numCommitLogWrites := int32(0)
	mockCommitLogWriter := commitLogWriter(commitLogWriterFn(func(
		ctx context.Context,
		series commitlog.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error {
		atomic.AddInt32(&numCommitLogWrites, 1)
		return nil
	}))

	shard := testDatabaseShard(t, opts)
	shard.commitLogWriter = mockCommitLogWriter
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.Write(ctx, ident.StringID("foo"), now, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("bar"), now, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), now, 3.0, xtime.Second, nil))

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
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("foo"), next, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("bar"), next, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), next, 3.0, xtime.Second, nil))

	written := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCommitLogWrites) == 6
	}, 2*time.Second)
	assert.True(t, written)

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
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockSize := namespace.NewIndexOptions().BlockSize()

	idx := NewMocknamespaceIndex(ctrl)
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
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, nowFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	opts := testDatabaseOptions()
	idx, err := newNamespaceIndexWithInsertQueueFn(md, newFn, opts.
		SetIndexOptions(opts.IndexOptions().SetInsertMode(index.InsertSync)))
	assert.NoError(t, err)

	testShardWriteTaggedSyncRefCount(t, idx)
	assert.NoError(t, idx.Close())
}

func testShardWriteTaggedSyncRefCount(t *testing.T, idx namespaceIndex) {
	numCommitLogWrites := int32(0)
	mockCommitLogWriter := commitLogWriter(commitLogWriterFn(func(
		ctx context.Context,
		series commitlog.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error {
		atomic.AddInt32(&numCommitLogWrites, 1)
		return nil
	}))

	now := time.Now()
	opts := testDatabaseOptions()

	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.commitLogWriter = mockCommitLogWriter
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"), ident.EmptyTagIterator, now, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("bar"), ident.EmptyTagIterator, now, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("baz"), ident.EmptyTagIterator, now, 3.0, xtime.Second, nil))

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
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"), ident.EmptyTagIterator, next, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("bar"), ident.EmptyTagIterator, next, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("baz"), ident.EmptyTagIterator, next, 3.0, xtime.Second, nil))

	written := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCommitLogWrites) == 6
	}, 2*time.Second)
	assert.True(t, written)

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

	numCommitLogWrites := int32(0)
	mockCommitLogWriter := commitLogWriter(commitLogWriterFn(func(
		ctx context.Context,
		series commitlog.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error {
		atomic.AddInt32(&numCommitLogWrites, 1)
		return nil
	}))

	now := time.Now()
	opts := testDatabaseOptions()
	opts = opts.SetInstrumentOptions(
		opts.InstrumentOptions().
			SetMetricsScope(scope).
			SetReportInterval(100 * time.Millisecond))

	shard := testDatabaseShard(t, opts)
	shard.commitLogWriter = mockCommitLogWriter
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.Write(ctx, ident.StringID("foo"), now, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("bar"), now, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), now, 3.0, xtime.Second, nil))

	inserted := xclock.WaitUntil(func() bool {
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
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("foo"), next, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("bar"), next, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.Write(ctx, ident.StringID("baz"), next, 3.0, xtime.Second, nil))

	written := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCommitLogWrites) == 6
	}, 2*time.Second)
	assert.True(t, written)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}

func TestShardWriteTaggedAsyncRefCountMockIndex(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	blockSize := namespace.NewIndexOptions().BlockSize()

	idx := NewMocknamespaceIndex(ctrl)
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

	testShardWriteTaggedAsyncRefCount(t, idx)
}

func TestShardWriteTaggedAsyncRefCountSyncIndex(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	newFn := func(fn nsIndexInsertBatchFn, nowFn clock.NowFn, s tally.Scope) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, nowFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithInsertQueueFn(md, newFn, testDatabaseOptions().
		SetIndexOptions(index.NewOptions().SetInsertMode(index.InsertSync)))
	assert.NoError(t, err)

	defer func() {
		assert.NoError(t, idx.Close())
	}()

	testShardWriteTaggedAsyncRefCount(t, idx)
}

func testShardWriteTaggedAsyncRefCount(t *testing.T, idx namespaceIndex) {
	testReporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)
	defer closer.Close()

	numCommitLogWrites := int32(0)
	mockCommitLogWriter := commitLogWriter(commitLogWriterFn(func(
		ctx context.Context,
		series commitlog.Series,
		datapoint ts.Datapoint,
		unit xtime.Unit,
		annotation ts.Annotation,
	) error {
		atomic.AddInt32(&numCommitLogWrites, 1)
		return nil
	}))

	now := time.Now()
	opts := testDatabaseOptions()
	opts = opts.SetInstrumentOptions(
		opts.InstrumentOptions().
			SetMetricsScope(scope).
			SetReportInterval(100 * time.Millisecond))

	shard := testDatabaseShardWithIndexFn(t, opts, idx)
	shard.commitLogWriter = mockCommitLogWriter
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"), ident.EmptyTagIterator, now, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("bar"), ident.EmptyTagIterator, now, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("baz"), ident.EmptyTagIterator, now, 3.0, xtime.Second, nil))

	inserted := xclock.WaitUntil(func() bool {
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
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("foo"), ident.EmptyTagIterator, next, 1.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("bar"), ident.EmptyTagIterator, next, 2.0, xtime.Second, nil))
	assert.NoError(t,
		shard.WriteTagged(ctx, ident.StringID("baz"), ident.EmptyTagIterator, next, 3.0, xtime.Second, nil))

	written := xclock.WaitUntil(func() bool {
		return atomic.LoadInt32(&numCommitLogWrites) == 6
	}, 5*time.Second)
	assert.True(t, written)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		assert.NoError(t, err)
		assert.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}
