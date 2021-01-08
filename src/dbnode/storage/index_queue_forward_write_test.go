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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/index"
	idxconvert "github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/m3ninx/doc"
	m3ninxidx "github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func generateOptionsNowAndBlockSize() (Options, time.Time, time.Duration) {
	idxOpts := testNamespaceIndexOptions().
		SetInsertMode(index.InsertSync).
		SetForwardIndexProbability(1).
		SetForwardIndexThreshold(1)

	opts := DefaultTestOptions().
		SetIndexOptions(idxOpts)

	var (
		retOpts        = opts.SeriesOptions().RetentionOptions()
		blockSize      = retOpts.BlockSize()
		bufferFuture   = retOpts.BufferFuture()
		bufferFragment = blockSize - time.Duration(float64(bufferFuture)*0.5)
		now            = time.Now().Truncate(blockSize).Add(bufferFragment)

		clockOptions = opts.ClockOptions()
	)

	clockOptions = clockOptions.SetNowFn(func() time.Time { return now })
	opts = opts.SetClockOptions(clockOptions)

	return opts, now, blockSize
}

func setupForwardIndex(
	t *testing.T,
	ctrl *gomock.Controller,
) (NamespaceIndex, time.Time, time.Duration) {
	newFn := func(
		fn nsIndexInsertBatchFn,
		md namespace.Metadata,
		nowFn clock.NowFn,
		s tally.Scope,
	) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, md, nowFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	opts, now, blockSize := generateOptionsNowAndBlockSize()
	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, opts)
	require.NoError(t, err)

	var (
		ts     = idx.(*nsIndex).state.latestBlock.StartTime()
		nextTs = ts.Add(blockSize)
		next   = ts.Truncate(blockSize).Add(blockSize)
		id     = ident.StringID("foo")
		tags   = ident.NewTags(
			ident.StringTag("name", "value"),
		)
		lifecycle = index.NewMockOnIndexSeries(ctrl)
	)

	gomock.InOrder(
		lifecycle.EXPECT().NeedsIndexUpdate(xtime.ToUnixNano(next)).Return(true),
		lifecycle.EXPECT().OnIndexPrepare(),

		lifecycle.EXPECT().OnIndexSuccess(xtime.ToUnixNano(ts)),
		lifecycle.EXPECT().OnIndexFinalize(xtime.ToUnixNano(ts)),

		lifecycle.EXPECT().OnIndexSuccess(xtime.ToUnixNano(nextTs)),
		lifecycle.EXPECT().OnIndexFinalize(xtime.ToUnixNano(nextTs)),
	)

	entry, doc := testWriteBatchEntry(id, tags, now, lifecycle)
	batch := testWriteBatch(entry, doc, testWriteBatchBlockSizeOption(blockSize))
	require.NoError(t, idx.WriteBatch(batch))

	return idx, now, blockSize
}

func TestNamespaceForwardIndexInsertQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	ctx := context.NewContext()
	defer ctx.Close()

	idx, now, blockSize := setupForwardIndex(t, ctrl)
	defer idx.Close()

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("name"), []byte("val.*"))
	require.NoError(t, err)

	// NB: query both the current and the next index block to ensure that the
	// write was correctly indexed to both.
	nextBlockTime := now.Add(blockSize)
	queryTimes := []time.Time{now, nextBlockTime}
	reader := docs.NewEncodedDocumentReader()
	for _, ts := range queryTimes {
		res, err := idx.Query(ctx, index.Query{Query: reQuery}, index.QueryOptions{
			StartInclusive: ts.Add(-1 * time.Minute),
			EndExclusive:   ts.Add(1 * time.Minute),
		})
		require.NoError(t, err)

		require.True(t, res.Exhaustive)
		results := res.Results
		require.Equal(t, "testns1", results.Namespace().String())

		d, ok := results.Map().Get(ident.BytesID("foo"))
		md, err := docs.MetadataFromDocument(d, reader)
		require.NoError(t, err)
		tags := idxconvert.ToSeriesTags(md, idxconvert.Opts{NoClone: true})

		require.True(t, ok)
		require.True(t, ident.NewTagIterMatcher(
			ident.MustNewTagStringsIterator("name", "value")).Matches(
			tags))
	}
}

func TestNamespaceForwardIndexAggregateQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	ctx := context.NewContext()
	defer ctx.Close()

	idx, now, blockSize := setupForwardIndex(t, ctrl)
	defer idx.Close()

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("name"), []byte("val.*"))
	require.NoError(t, err)

	// NB: query both the current and the next index block to ensure that the
	// write was correctly indexed to both.
	nextBlockTime := now.Add(blockSize)
	queryTimes := []time.Time{now, nextBlockTime}
	for _, ts := range queryTimes {
		res, err := idx.AggregateQuery(ctx, index.Query{Query: reQuery},
			index.AggregationOptions{
				QueryOptions: index.QueryOptions{
					StartInclusive: ts.Add(-1 * time.Minute),
					EndExclusive:   ts.Add(1 * time.Minute),
				},
			},
		)
		require.NoError(t, err)

		require.True(t, res.Exhaustive)
		results := res.Results
		require.Equal(t, "testns1", results.Namespace().String())

		rMap := results.Map()
		require.Equal(t, 1, rMap.Len())
		seenIters, found := rMap.Get(ident.StringID("name"))
		require.True(t, found)

		vMap := seenIters.Map()
		require.Equal(t, 1, vMap.Len())
		require.True(t, vMap.Contains(ident.StringID("value")))
	}
}

func TestNamespaceForwardIndexWideQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 5*time.Second)()

	ctx := context.NewContext()
	defer ctx.Close()

	idx, now, blockSize := setupForwardIndex(t, ctrl)
	defer idx.Close()

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("name"), []byte("val.*"))
	require.NoError(t, err)

	// NB: query both the current and the next index block to ensure that the
	// write was correctly indexed to both.
	nextBlockTime := now.Add(blockSize)
	queryTimes := []time.Time{
		now.Truncate(blockSize),
		nextBlockTime.Truncate(blockSize),
	}
	for _, ts := range queryTimes {
		collector := make(chan *ident.IDBatch)
		doneCh := make(chan struct{})
		expectedBatchIDs := [][]string{{"foo"}}
		go func() {
			i := 0
			for b := range collector {
				batchStr := make([]string, 0, len(b.ShardIDs))
				for _, shardID := range b.ShardIDs {
					batchStr = append(batchStr, shardID.ID.String())
				}

				withinIndex := i < len(expectedBatchIDs)
				assert.True(t, withinIndex)
				if withinIndex {
					assert.Equal(t, expectedBatchIDs[i], batchStr)
				}

				b.Processed()
				i++
			}
			doneCh <- struct{}{}
		}()

		opts, err := index.NewWideQueryOptions(
			ts, 5, time.Hour*2, nil, index.IterationOptions{})
		require.NoError(t, err)

		err = idx.WideQuery(ctx, index.Query{Query: reQuery}, collector, opts)
		assert.NoError(t, err)
		<-doneCh
	}
}

func setupMockBlock(
	t *testing.T,
	bl *index.MockBlock,
	ts time.Time,
	id ident.ID,
	tag ident.Tag,
	lifecycle index.OnIndexSeries,
) {
	bl.EXPECT().
		WriteBatch(gomock.Any()).
		Return(index.WriteBatchResult{}, nil).
		Do(func(batch *index.WriteBatch) {
			docs := batch.PendingDocs()
			require.Equal(t, 1, len(docs))
			require.Equal(t, doc.Metadata{
				ID:     id.Bytes(),
				Fields: doc.Fields{{Name: tag.Name.Bytes(), Value: tag.Value.Bytes()}},
			}, docs[0])
			entries := batch.PendingEntries()
			require.Equal(t, 1, len(entries))
			require.True(t, entries[0].Timestamp.Equal(ts))
			require.True(t, entries[0].OnIndexSeries == lifecycle) // Just ptr equality
		})
}

func createMockBlocks(
	ctrl *gomock.Controller,
	blockStart time.Time,
	nextBlockStart time.Time,
) (*index.MockBlock, *index.MockBlock, index.NewBlockFn) {
	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	mockBlock.EXPECT().Close().Return(nil)
	mockBlock.EXPECT().StartTime().Return(blockStart).AnyTimes()

	futureBlock := index.NewMockBlock(ctrl)
	futureBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	futureBlock.EXPECT().Close().Return(nil)
	futureBlock.EXPECT().StartTime().Return(nextBlockStart).AnyTimes()

	var madeBlock, madeFuture bool
	newBlockFn := func(
		ts time.Time,
		md namespace.Metadata,
		_ index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if ts.Equal(blockStart) {
			if madeBlock {
				return mockBlock, errors.New("already created initial block")
			}
			madeBlock = true
			return mockBlock, nil
		} else if ts.Equal(nextBlockStart) {
			if madeFuture {
				return nil, errors.New("already created forward block")
			}
			madeFuture = true
			return futureBlock, nil
		}
		return nil, fmt.Errorf("no block starting at %s; must start at %s or %s",
			ts, blockStart, nextBlockStart)
	}

	return mockBlock, futureBlock, newBlockFn
}

func TestNamespaceIndexForwardWrite(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	opts, now, blockSize := generateOptionsNowAndBlockSize()
	blockStart := now.Truncate(blockSize)
	futureStart := blockStart.Add(blockSize)
	mockBlock, futureBlock, newBlockFn := createMockBlocks(ctrl, blockStart, futureStart)

	md := testNamespaceMetadata(blockSize, 4*time.Hour)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	id := ident.StringID("foo")
	tag := ident.StringTag("name", "value")
	tags := ident.NewTags(tag)
	lifecycle := index.NewMockOnIndexSeries(ctrl)

	var (
		ts   = idx.(*nsIndex).state.latestBlock.StartTime()
		next = ts.Truncate(blockSize).Add(blockSize)
	)

	lifecycle.EXPECT().NeedsIndexUpdate(xtime.ToUnixNano(next)).Return(true)
	lifecycle.EXPECT().OnIndexPrepare()

	setupMockBlock(t, mockBlock, now, id, tag, lifecycle)
	setupMockBlock(t, futureBlock, futureStart, id, tag, lifecycle)

	batch := index.NewWriteBatch(index.WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(testWriteBatchEntry(id, tags, now, lifecycle))
	require.NoError(t, idx.WriteBatch(batch))
}

func TestNamespaceIndexForwardWriteCreatesBlock(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{T: t})
	defer ctrl.Finish()

	opts, now, blockSize := generateOptionsNowAndBlockSize()
	blockStart := now.Truncate(blockSize)
	futureStart := blockStart.Add(blockSize)
	mockBlock, futureBlock, newBlockFn := createMockBlocks(ctrl, blockStart, futureStart)

	md := testNamespaceMetadata(blockSize, 4*time.Hour)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	id := ident.StringID("foo")
	tag := ident.StringTag("name", "value")
	tags := ident.NewTags(tag)
	lifecycle := index.NewMockOnIndexSeries(ctrl)

	var (
		ts   = idx.(*nsIndex).state.latestBlock.StartTime()
		next = ts.Truncate(blockSize).Add(blockSize)
	)

	lifecycle.EXPECT().NeedsIndexUpdate(xtime.ToUnixNano(next)).Return(true)
	lifecycle.EXPECT().OnIndexPrepare()

	setupMockBlock(t, mockBlock, now, id, tag, lifecycle)
	setupMockBlock(t, futureBlock, futureStart, id, tag, lifecycle)

	entry, doc := testWriteBatchEntry(id, tags, now, lifecycle)
	batch := testWriteBatch(entry, doc, testWriteBatchBlockSizeOption(blockSize))
	require.NoError(t, idx.WriteBatch(batch))
}

func TestShardForwardWriteTaggedSyncRefCountSyncIndex(t *testing.T) {
	testShardForwardWriteTaggedRefCountIndex(t, index.InsertSync, false)
}

func TestShardForwardWriteTaggedAsyncRefCountSyncIndex(t *testing.T) {
	testShardForwardWriteTaggedRefCountIndex(t, index.InsertAsync, true)
}

func testShardForwardWriteTaggedRefCountIndex(
	t *testing.T,
	syncType index.InsertMode,
	async bool,
) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()
	newFn := func(
		fn nsIndexInsertBatchFn,
		md namespace.Metadata,
		nowFn clock.NowFn,
		s tally.Scope,
	) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, md, nowFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	opts, now, blockSize := generateOptionsNowAndBlockSize()
	opts = opts.SetIndexOptions(opts.IndexOptions().SetInsertMode(syncType))

	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	next := now.Truncate(blockSize).Add(blockSize)
	if async {
		testShardForwardWriteTaggedAsyncRefCount(t, now, next, idx, opts)
	} else {
		testShardForwardWriteTaggedSyncRefCount(t, now, next, idx, opts)
	}
}

func writeToShard(
	ctx context.Context,
	t *testing.T,
	shard *dbShard,
	idx NamespaceIndex,
	now time.Time,
	id string,
	shouldWrite bool,
) {
	tag := ident.Tag{Name: ident.StringID(id), Value: ident.StringID("")}
	idTags := ident.NewTags(tag)
	iter := ident.NewTagsIterator(idTags)
	seriesWrite, err := shard.WriteTagged(ctx, ident.StringID(id), iter, now,
		1.0, xtime.Second, nil, series.WriteOptions{
			TruncateType: series.TypeBlock,
			TransformOptions: series.WriteTransformOptions{
				ForceValueEnabled: true,
				ForceValue:        1,
			},
		})
	require.NoError(t, err)
	require.Equal(t, shouldWrite, seriesWrite.WasWritten)
	if seriesWrite.NeedsIndex {
		err = idx.WritePending([]writes.PendingIndexInsert{
			seriesWrite.PendingIndexInsert,
		})
		require.NoError(t, err)
	}
}

func verifyShard(
	ctx context.Context,
	t *testing.T,
	idx NamespaceIndex,
	now time.Time,
	next time.Time,
	id string,
) {
	allQueriesSuccess := clock.WaitUntil(func() bool {
		query := m3ninxidx.NewFieldQuery([]byte(id))
		// check current index block for series
		res, err := idx.Query(ctx, index.Query{Query: query}, index.QueryOptions{
			StartInclusive: now,
			EndExclusive:   next,
		})
		require.NoError(t, err)
		if res.Results.Size() != 1 {
			return false
		}

		// check next index block for series
		res, err = idx.Query(ctx, index.Query{Query: query}, index.QueryOptions{
			StartInclusive: next.Add(1 * time.Minute),
			EndExclusive:   next.Add(5 * time.Minute),
		})
		require.NoError(t, err)
		if res.Results.Size() != 1 {
			return false
		}

		// check across both index blocks to ensure only a single ID is returned.
		res, err = idx.Query(ctx, index.Query{Query: query}, index.QueryOptions{
			StartInclusive: now,
			EndExclusive:   next.Add(5 * time.Minute),
		})
		require.NoError(t, err)
		if res.Results.Size() != 1 {
			return false
		}

		return true
	}, 5*time.Second)
	require.True(t, allQueriesSuccess)
}

func writeToShardAndVerify(
	ctx context.Context,
	t *testing.T,
	shard *dbShard,
	idx NamespaceIndex,
	now time.Time,
	next time.Time,
	id string,
	shouldWrite bool,
) {
	writeToShard(ctx, t, shard, idx, now, id, shouldWrite)
	verifyShard(ctx, t, idx, now, next, id)
}

func testShardForwardWriteTaggedSyncRefCount(
	t *testing.T,
	now time.Time,
	next time.Time,
	idx NamespaceIndex,
	opts Options,
) {
	shard := testDatabaseShardWithIndexFn(t, opts, idx, false)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(false))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	writeToShardAndVerify(ctx, t, shard, idx, now, next, "foo", true)
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "bar", true)
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "baz", true)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		require.NoError(t, err)
		require.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}

	// move the time the point is written to ensure truncation works.
	now = now.Add(1)
	// write already inserted series
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "foo", false)
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "bar", false)
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "baz", false)

	// // ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		require.NoError(t, err)
		require.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}

func testShardForwardWriteTaggedAsyncRefCount(
	t *testing.T,
	now time.Time,
	next time.Time,
	idx NamespaceIndex,
	opts Options,
) {
	testReporterOpts := xmetrics.NewTestStatsReporterOptions()
	testReporter := xmetrics.NewTestStatsReporter(testReporterOpts)
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)
	defer closer.Close()
	opts = opts.SetInstrumentOptions(
		opts.InstrumentOptions().
			SetMetricsScope(scope).
			SetReportInterval(100 * time.Millisecond))

	shard := testDatabaseShardWithIndexFn(t, opts, idx, false)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(true))
	defer shard.Close()

	ctx := context.NewContext()
	defer ctx.Close()

	writeToShard(ctx, t, shard, idx, now, "foo", true)
	writeToShard(ctx, t, shard, idx, now, "bar", true)
	writeToShard(ctx, t, shard, idx, now, "baz", true)

	verifyShard(ctx, t, idx, now, next, "foo")
	verifyShard(ctx, t, idx, now, next, "bar")
	verifyShard(ctx, t, idx, now, next, "baz")

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		require.NoError(t, err)
		require.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}

	// write already inserted series. This should have no effect.
	now = now.Add(1)
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "foo", false)
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "bar", false)
	writeToShardAndVerify(ctx, t, shard, idx, now, next, "baz", false)

	// ensure all entries have no references left
	for _, id := range []string{"foo", "bar", "baz"} {
		shard.Lock()
		entry, _, err := shard.lookupEntryWithLock(ident.StringID(id))
		shard.Unlock()
		require.NoError(t, err)
		require.Equal(t, int32(0), entry.ReaderWriterCount(), id)
	}
}
