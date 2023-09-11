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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	m3dberrors "github.com/m3db/m3/src/dbnode/storage/errors"
	"github.com/m3db/m3/src/dbnode/storage/index"
	idxconvert "github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/doc"
	m3ninxidx "github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/resource"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testNamespaceIndexOptions() index.Options {
	return DefaultTestOptions().IndexOptions()
}

func newTestNamespaceIndex(t *testing.T, ctrl *gomock.Controller) (NamespaceIndex, *MocknamespaceIndexInsertQueue) {
	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(
		fn nsIndexInsertBatchFn,
		md namespace.Metadata,
		nowFn clock.NowFn,
		coreFn xsync.CoreFn,
		s tally.Scope,
	) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(nil)
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, DefaultTestOptions())
	assert.NoError(t, err)
	return idx, q
}

func TestNamespaceIndexHappyPath(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(
		fn nsIndexInsertBatchFn,
		md namespace.Metadata,
		nowFn clock.NowFn,
		coreFn xsync.CoreFn,
		s tally.Scope,
	) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(nil)

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, DefaultTestOptions())
	assert.NoError(t, err)
	assert.NotNil(t, idx)

	q.EXPECT().Stop().Return(nil)
	assert.NoError(t, idx.Close())
}

func TestNamespaceIndexStartErr(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(
		fn nsIndexInsertBatchFn,
		md namespace.Metadata,
		nowFn clock.NowFn,
		coreFn xsync.CoreFn,
		s tally.Scope,
	) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(fmt.Errorf("random err"))
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, DefaultTestOptions())
	assert.Error(t, err)
	assert.Nil(t, idx)
}

func TestNamespaceIndexStopErr(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	q := NewMocknamespaceIndexInsertQueue(ctrl)
	newFn := func(
		fn nsIndexInsertBatchFn,
		md namespace.Metadata,
		nowFn clock.NowFn,
		coreFn xsync.CoreFn,
		s tally.Scope,
	) namespaceIndexInsertQueue {
		return q
	}
	q.EXPECT().Start().Return(nil)

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, DefaultTestOptions())
	assert.NoError(t, err)
	assert.NotNil(t, idx)

	q.EXPECT().Stop().Return(fmt.Errorf("random err"))
	assert.Error(t, idx.Close())
}

func TestNamespaceIndexWriteAfterClose(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dbIdx, q := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("name", "value"),
	)

	q.EXPECT().Stop().Return(nil)
	assert.NoError(t, idx.Close())

	now := xtime.Now()

	lifecycle := doc.NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize(now.Truncate(idx.blockSize))
	lifecycle.EXPECT().IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).
		Return(false).
		AnyTimes()
	entry, document := testWriteBatchEntry(id, tags, now, lifecycle)
	assert.Error(t, idx.WriteBatch(testWriteBatch(entry, document,
		testWriteBatchBlockSizeOption(idx.blockSize))))
}

func TestNamespaceIndexWriteQueueError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dbIdx, q := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	id := ident.StringID("foo")
	tags := ident.NewTags(
		ident.StringTag("name", "value"),
	)

	n := xtime.Now()
	lifecycle := doc.NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize(n.Truncate(idx.blockSize))
	lifecycle.EXPECT().IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).Return(false)
	q.EXPECT().
		InsertBatch(gomock.Any()).
		Return(nil, fmt.Errorf("random err"))
	entry, document := testWriteBatchEntry(id, tags, n, lifecycle)
	assert.Error(t, idx.WriteBatch(testWriteBatch(entry, document,
		testWriteBatchBlockSizeOption(idx.blockSize))))
}

func TestNamespaceIndexInsertOlderThanRetentionPeriod(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		nowLock sync.Mutex
		now     = xtime.Now()
		nowFn   = func() time.Time {
			nowLock.Lock()
			n := now
			nowLock.Unlock()
			return n.ToTime()
		}
	)

	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)

	opts := testNamespaceIndexOptions().SetInsertMode(index.InsertSync)
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))
	dbIdx, err := newNamespaceIndex(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, DefaultTestOptions().SetIndexOptions(opts))
	assert.NoError(t, err)

	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	var (
		id   = ident.StringID("foo")
		tags = ident.NewTags(
			ident.StringTag("name", "value"),
		)
		lifecycle = doc.NewMockOnIndexSeries(ctrl)
	)

	tooOld := now.Add(-1 * idx.bufferPast).Add(-1 * time.Second)
	lifecycle.EXPECT().OnIndexFinalize(tooOld.Truncate(idx.blockSize))
	lifecycle.EXPECT().IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).
		Return(false).
		AnyTimes()
	entry, document := testWriteBatchEntry(id, tags, tooOld, lifecycle)
	batch := testWriteBatch(entry, document, testWriteBatchBlockSizeOption(idx.blockSize))

	assert.Error(t, idx.WriteBatch(batch))

	verified := 0
	batch.ForEach(func(
		idx int,
		entry index.WriteBatchEntry,
		doc doc.Metadata,
		result index.WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.Equal(t, m3dberrors.ErrTooPast, result.Err)
	})
	require.Equal(t, 1, verified)

	tooNew := now.Add(1 * idx.bufferFuture).Add(1 * time.Second)
	lifecycle.EXPECT().OnIndexFinalize(tooNew.Truncate(idx.blockSize))
	entry, document = testWriteBatchEntry(id, tags, tooNew, lifecycle)
	batch = testWriteBatch(entry, document, testWriteBatchBlockSizeOption(idx.blockSize))
	assert.Error(t, idx.WriteBatch(batch))

	verified = 0
	batch.ForEach(func(
		idx int,
		entry index.WriteBatchEntry,
		doc doc.Metadata,
		result index.WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.Equal(t, m3dberrors.ErrTooFuture, result.Err)
	})
	require.Equal(t, 1, verified)

	assert.NoError(t, dbIdx.Close())
}

func TestNamespaceIndexInsertQueueInteraction(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dbIdx, q := newTestNamespaceIndex(t, ctrl)
	idx, ok := dbIdx.(*nsIndex)
	assert.True(t, ok)

	var (
		id   = ident.StringID("foo")
		tags = ident.NewTags(
			ident.StringTag("name", "value"),
		)
	)

	now := xtime.Now()

	var wg sync.WaitGroup
	lifecycle := doc.NewMockOnIndexSeries(ctrl)
	q.EXPECT().InsertBatch(gomock.Any()).Return(&wg, nil)
	lifecycle.EXPECT().IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).
		Return(false).
		AnyTimes()
	assert.NoError(t, idx.WriteBatch(testWriteBatch(testWriteBatchEntry(id,
		tags, now, lifecycle))))
}

func setupIndex(t *testing.T,
	ctrl *gomock.Controller,
	now xtime.UnixNano,
	expectAggregateQuery bool,
) NamespaceIndex {
	newFn := func(
		fn nsIndexInsertBatchFn,
		md namespace.Metadata,
		nowFn clock.NowFn,
		coreFn xsync.CoreFn,
		s tally.Scope,
	) namespaceIndexInsertQueue {
		q := newNamespaceIndexInsertQueue(fn, md, nowFn, coreFn, s)
		q.(*nsIndexInsertQueue).indexBatchBackoff = 10 * time.Millisecond
		return q
	}
	md, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	idx, err := newNamespaceIndexWithInsertQueueFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newFn, DefaultTestOptions().
			SetIndexOptions(testNamespaceIndexOptions().
				SetInsertMode(index.InsertSync)))
	assert.NoError(t, err)

	var (
		blockSize = idx.(*nsIndex).blockSize
		ts        = idx.(*nsIndex).state.latestBlock.StartTime()
		id        = ident.StringID("foo")
		tags      = ident.NewTags(
			ident.StringTag("name", "value"),
		)
		lifecycleFns = doc.NewMockOnIndexSeries(ctrl)
	)

	closer := &resource.NoopCloser{}
	lifecycleFns.EXPECT().ReconciledOnIndexSeries().Return(lifecycleFns, closer, false).AnyTimes()
	lifecycleFns.EXPECT().OnIndexFinalize(ts)
	lifecycleFns.EXPECT().OnIndexSuccess(ts)
	lifecycleFns.EXPECT().IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).Return(false)

	if !expectAggregateQuery {
		lifecycleFns.EXPECT().IndexedRange().Return(ts, ts)
		lifecycleFns.EXPECT().IndexedForBlockStart(ts).Return(true)
	}

	entry, doc := testWriteBatchEntry(id, tags, now, lifecycleFns)
	batch := testWriteBatch(entry, doc, testWriteBatchBlockSizeOption(blockSize))
	assert.NoError(t, idx.WriteBatch(batch))

	return idx
}

func TestNamespaceIndexInsertQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	ctx := context.NewBackground()
	defer ctx.Close()

	now := xtime.Now()
	idx := setupIndex(t, ctrl, now, false)
	defer idx.Close()

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("name"), []byte("val.*"))
	assert.NoError(t, err)
	res, err := idx.Query(ctx, index.Query{Query: reQuery}, index.QueryOptions{
		StartInclusive: now.Add(-1 * time.Minute),
		EndExclusive:   now.Add(1 * time.Minute),
	})
	require.NoError(t, err)

	assert.True(t, res.Exhaustive)
	results := res.Results
	assert.Equal(t, "testns1", results.Namespace().String())

	reader := docs.NewEncodedDocumentReader()
	d, ok := results.Map().Get(ident.BytesID("foo"))
	md, err := docs.MetadataFromDocument(d, reader)
	require.NoError(t, err)
	tags := idxconvert.ToSeriesTags(md, idxconvert.Opts{NoClone: true})

	assert.True(t, ok)
	assert.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("name", "value")).Matches(
		tags))
}

func TestNamespaceIndexInsertAggregateQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	ctx := context.NewBackground()
	defer ctx.Close()

	now := xtime.Now()
	idx := setupIndex(t, ctrl, now, true)
	defer idx.Close()

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("name"), []byte("val.*"))
	assert.NoError(t, err)
	res, err := idx.AggregateQuery(ctx, index.Query{Query: reQuery},
		index.AggregationOptions{
			QueryOptions: index.QueryOptions{
				StartInclusive: now.Add(-1 * time.Minute),
				EndExclusive:   now.Add(1 * time.Minute),
			},
		},
	)
	require.NoError(t, err)

	assert.True(t, res.Exhaustive)
	results := res.Results
	assert.Equal(t, "testns1", results.Namespace().String())

	rMap := results.Map()
	require.Equal(t, 1, rMap.Len())
	seenIters, found := rMap.Get(ident.StringID("name"))
	require.True(t, found)

	vMap := seenIters.Map()
	require.Equal(t, 1, vMap.Len())
	assert.True(t, vMap.Contains(ident.StringID("value")))
}
