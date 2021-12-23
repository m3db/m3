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

package index

import (
	stdlibctx "context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/test"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/resource"
	"github.com/m3db/m3/src/x/tallytest"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	defaultQuery = Query{
		Query: idx.NewTermQuery([]byte("foo"), []byte("bar")),
	}

	emptyLogFields = []opentracinglog.Field{}
)

func newTestNSMetadata(t require.TestingT) namespace.Metadata {
	ropts := retention.NewOptions().
		SetBlockSize(time.Hour).
		SetRetentionPeriod(24 * time.Hour)
	iopts := namespace.NewIndexOptions().
		SetEnabled(true).
		SetBlockSize(time.Hour)
	md, err := namespace.NewMetadata(ident.StringID("testNs"),
		namespace.NewOptions().SetRetentionOptions(ropts).SetIndexOptions(iopts))
	require.NoError(t, err)
	return md
}

func TestBlockCtor(t *testing.T) {
	md := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	b, err := NewBlock(start, md, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	require.Equal(t, start, b.StartTime())
	require.Equal(t, start.Add(time.Hour), b.EndTime())
	require.NoError(t, b.Close())
	require.Error(t, b.Close())
}

// nolint: unparam
func testWriteBatchOptionsWithBlockSize(d time.Duration) WriteBatchOptions {
	return WriteBatchOptions{
		IndexBlockSize: d,
	}
}

func TestBlockWriteAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	b, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	require.NoError(t, b.Close())

	lifecycle := doc.NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: lifecycle,
	}, doc.Metadata{})

	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(0), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Metadata,
		result WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.Equal(t, errUnableToWriteBlockClosed, result.Err)
	})
	require.Equal(t, 1, verified)
}

func TestBlockWriteAfterSeal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	testMD := newTestNSMetadata(t)

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	b, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	require.NoError(t, b.Seal())

	lifecycle := doc.NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: lifecycle,
	}, doc.Metadata{})

	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(0), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Metadata,
		result WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.Equal(t, errUnableToWriteBlockSealed, result.Err)
	})
	require.Equal(t, 1, verified)
}

func TestBlockWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, blk.Close())
	}()

	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)
	h2.EXPECT().OnIndexSuccess(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)
}

func TestBlockWriteActualSegmentPartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, md, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, doc.Metadata{})
	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		_ doc.Metadata,
		result WriteBatchEntryResult,
	) {
		verified++
		if idx == 0 {
			require.NoError(t, result.Err)
		} else {
			require.Error(t, result.Err)
			require.Equal(t, doc.ErrEmptyDocument, result.Err)
		}
	})
	require.Equal(t, 2, verified)
}

func TestBlockWritePartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	md := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, md, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, doc.Metadata{})

	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Metadata,
		result WriteBatchEntryResult,
	) {
		verified++
		if idx == 0 {
			require.NoError(t, result.Err)
		} else {
			require.Error(t, result.Err)
		}
	})
	require.Equal(t, 2, verified)
}

func TestBlockQueryAfterClose(t *testing.T) {
	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	b, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	require.Equal(t, start, b.StartTime())
	require.Equal(t, start.Add(time.Hour), b.EndTime())
	require.NoError(t, b.Close())

	_, err = b.QueryIter(context.NewBackground(), defaultQuery)
	require.Error(t, err)
}

func TestBlockQueryWithCancelledQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Done().Return(false),
		exec.EXPECT().Close().Return(nil),
	)

	require.Equal(t, start, b.StartTime())
	require.Equal(t, start.Add(time.Hour), b.EndTime())

	// Precancel query.
	stdCtx, cancel := stdlibctx.WithCancel(stdlibctx.Background())
	cancel()
	ctx := context.NewWithGoContext(stdCtx)
	defer ctx.BlockingClose()

	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{}, queryIter, results, time.Now().Add(time.Minute), emptyLogFields)
	require.Error(t, err)
	require.Equal(t, stdlibctx.Canceled, err)
}

func TestBlockQueryExecutorError(t *testing.T) {
	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return nil, fmt.Errorf("random-err")
	}

	_, err = b.QueryIter(context.NewBackground(), defaultQuery)
	require.Error(t, err)
}

func TestBlockQuerySegmentReaderError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg := segment.NewMockSegment(ctrl)
	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg, testOpts)}
	randErr := fmt.Errorf("random-err")
	seg.EXPECT().Reader().Return(nil, randErr)

	_, err = b.QueryIter(context.NewBackground(), defaultQuery)
	require.Equal(t, randErr, err)
}

func TestBlockQueryAddResultsSegmentsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	seg2 := segment.NewMockMutableSegment(ctrl)
	seg3 := segment.NewMockMutableSegment(ctrl)

	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, testOpts)}
	b.shardRangesSegmentsByVolumeType = map[idxpersist.IndexVolumeType][]blockShardRangesSegments{
		idxpersist.DefaultIndexVolumeType: {
			{segments: []segment.Segment{seg2, seg3}},
		},
	}

	r1 := segment.NewMockReader(ctrl)
	seg1.EXPECT().Reader().Return(r1, nil)
	r1.EXPECT().Close().Return(nil)

	r2 := segment.NewMockReader(ctrl)
	seg2.EXPECT().Reader().Return(r2, nil)
	r2.EXPECT().Close().Return(nil)

	randErr := fmt.Errorf("random-err")
	seg3.EXPECT().Reader().Return(nil, randErr)

	_, err = b.QueryIter(context.NewBackground(), defaultQuery)
	require.Equal(t, randErr, err)
}

func TestBlockMockQueryExecutorExecError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	// dIter:= doc.NewMockIterator(ctrl)
	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("randomerr")),
		exec.EXPECT().Close(),
	)

	_, err = b.QueryIter(context.NewBackground(), defaultQuery)
	require.Error(t, err)
}

func TestBlockMockQueryExecutorExecIterErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc1())),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(fmt.Errorf("randomerr")),
		dIter.EXPECT().Done().Return(true),
		exec.EXPECT().Close(),
	)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)

	err = b.QueryWithIter(ctx, QueryOptions{}, queryIter,
		NewQueryResults(nil, QueryResultsOptions{}, testOpts), time.Now().Add(time.Minute), emptyLogFields)
	require.Error(t, err)

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockMockQueryExecutorExecLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc1())),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Done().Return(true),
		exec.EXPECT().Close().Return(nil),
	)
	limit := 1
	results := NewQueryResults(nil,
		QueryResultsOptions{SizeLimit: limit}, testOpts)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{SeriesLimit: limit}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	require.Equal(t, 1, results.Map().Len())
	d, ok := results.Map().Get(testDoc1().ID)
	require.True(t, ok)

	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockMockQuerySeriesLimitNonExhaustive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc1())),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Done().Return(true),
		exec.EXPECT().Close().Return(nil),
	)
	limit := 1
	results := NewQueryResults(nil, QueryResultsOptions{SizeLimit: 1}, testOpts)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{SeriesLimit: limit}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	require.Equal(t, 1, results.Map().Len())

	d, ok := results.Map().Get(testDoc1().ID)
	require.True(t, ok)

	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockMockQuerySeriesLimitExhaustive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc1())),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Done().Return(true),
		exec.EXPECT().Close().Return(nil),
	)
	limit := 2
	results := NewQueryResults(nil,
		QueryResultsOptions{SizeLimit: limit}, testOpts)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{SeriesLimit: limit}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	rMap := results.Map()
	require.Equal(t, 1, rMap.Len())
	d, ok := rMap.Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockMockQueryDocsLimitNonExhaustive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc1())),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Done().Return(true),
		exec.EXPECT().Close().Return(nil),
	)
	docsLimit := 1
	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{DocsLimit: docsLimit}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	require.Equal(t, 1, results.Map().Len())
	d, ok := results.Map().Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockMockQueryDocsLimitExhaustive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc1())),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Done().Return(false),
		exec.EXPECT().Close().Return(nil),
	)
	docsLimit := 2
	results := NewQueryResults(nil,
		QueryResultsOptions{}, testOpts)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{DocsLimit: docsLimit}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	rMap := results.Map()
	require.Equal(t, 1, rMap.Len())
	d, ok := rMap.Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockMockQueryMergeResultsMapLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)
	require.NoError(t, b.Seal())

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	limit := 1
	results := NewQueryResults(nil,
		QueryResultsOptions{SizeLimit: limit}, testOpts)
	_, _, err = results.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(testDoc1()),
	})
	require.NoError(t, err)

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Done().Return(true),
		exec.EXPECT().Close().Return(nil),
	)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{SeriesLimit: limit}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	rMap := results.Map()
	require.Equal(t, 1, rMap.Len())
	d, ok := rMap.Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockQueryWithIterMetrics(t *testing.T) {
	block, err := NewBlock(xtime.Now().Truncate(time.Hour),
		newTestNSMetadata(t), BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"),
		testOpts)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()

	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	_, _, err = results.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(testDoc1()),
		doc.NewDocumentFromMetadata(testDoc2()),
	})
	require.NoError(t, err)

	queryIter := NewMockQueryIterator(ctrl)
	notImportantInt := 0
	gomock.InOrder(
		queryIter.EXPECT().Next(ctx).Return(true),
		queryIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc3())),
		queryIter.EXPECT().Next(ctx).Return(false),
		queryIter.EXPECT().Err().Return(nil),

		// Iterator returns one document, so expect numOfSeries that have been added to the results object to be 1
		// and number of documents to be 1
		queryIter.EXPECT().AddSeries(gomock.Eq(1)),
		queryIter.EXPECT().AddDocs(gomock.Eq(1)),

		queryIter.EXPECT().Done().Return(true),
		queryIter.EXPECT().Counts().Return(notImportantInt, notImportantInt),
	)

	err = block.QueryWithIter(ctx, QueryOptions{}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)
}

func TestBlockMockQueryMergeResultsDupeID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorWithRLockFn = func() (search.Executor, error) {
		return exec, nil
	}

	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	_, _, err = results.AddDocuments([]doc.Document{
		doc.NewDocumentFromMetadata(testDoc1()),
	})
	require.NoError(t, err)

	dIter := doc.NewMockQueryDocIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc1DupeID())),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(doc.NewDocumentFromMetadata(testDoc2())),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Done().Return(true),
		exec.EXPECT().Close().Return(nil),
	)

	ctx := context.NewBackground()

	queryIter, err := b.QueryIter(ctx, defaultQuery)
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	rMap := results.Map()
	require.Equal(t, 2, rMap.Len())
	d, ok := rMap.Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	d, ok = rMap.Get(testDoc2().ID)
	require.True(t, ok)
	t2 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		t2))

	// NB(r): Make sure to call finalizers blockingly (to finish
	// the expected close calls)
	ctx.BlockingClose()
}

func TestBlockAddResultsAddsSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.NoError(t, b.AddResults(results))
	shardRangesSegments := b.shardRangesSegmentsByVolumeType[idxpersist.DefaultIndexVolumeType]
	require.Equal(t, 1, len(shardRangesSegments))

	require.Equal(t, seg1, shardRangesSegments[0].segments[0])
}

func TestBlockAddResultsAfterCloseFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Close())

	seg1 := segment.NewMockMutableSegment(ctrl)
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.Error(t, blk.AddResults(results))
}

func TestBlockAddResultsAfterSealWorks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Seal())

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.NoError(t, b.AddResults(results))
	shardRangesSegments := b.shardRangesSegmentsByVolumeType[idxpersist.DefaultIndexVolumeType]
	require.Equal(t, 1, len(shardRangesSegments))

	require.Equal(t, seg1, shardRangesSegments[0].segments[0])
}

func TestBlockTickSingleSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockSegment(ctrl)
	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, testOpts)}
	seg1.EXPECT().Size().Return(int64(10))

	result, err := blk.Tick(nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.NumSegments)
	require.Equal(t, int64(10), result.NumDocs)
}

func TestBlockTickMultipleSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockSegment(ctrl)
	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, testOpts)}
	seg1.EXPECT().Size().Return(int64(10))

	seg2 := segment.NewMockMutableSegment(ctrl)
	seg2.EXPECT().Size().Return(int64(20))
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.NoError(t, b.AddResults(results))

	result, err := blk.Tick(nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.NumSegments)
	require.Equal(t, int64(30), result.NumDocs)
}

func TestBlockTickAfterSeal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Seal())

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockSegment(ctrl)
	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, testOpts)}
	seg1.EXPECT().Size().Return(int64(10))

	result, err := blk.Tick(nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.NumSegments)
	require.Equal(t, int64(10), result.NumDocs)
}

func TestBlockTickAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Close())

	_, err = blk.Tick(nil)
	require.Error(t, err)
}

func TestBlockAddResultsRangeCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start.Add(-1*time.Minute), start, 1, 2, 3)))
	require.Error(t, b.AddResults(results))

	results = result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(2*time.Hour), 1, 2, 3)))
	require.Error(t, b.AddResults(results))
}

func TestBlockAddResultsCoversCurrentData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.NoError(t, b.AddResults(results))

	seg2 := segment.NewMockMutableSegment(ctrl)
	seg1.EXPECT().Close().Return(nil)
	results = result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3, 4)))
	require.NoError(t, b.AddResults(results))

	require.NoError(t, b.Seal())
	seg2.EXPECT().Close().Return(nil)
	require.NoError(t, b.Close())
}

func TestBlockAddResultsDoesNotCoverCurrentData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.NoError(t, b.AddResults(results))

	seg2 := segment.NewMockMutableSegment(ctrl)
	results = result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 5)))
	require.NoError(t, b.AddResults(results))

	require.NoError(t, b.Seal())

	seg1.EXPECT().Close().Return(nil)
	seg2.EXPECT().Close().Return(nil)
	require.NoError(t, b.Close())
}

func TestBlockNeedsMutableSegmentsEvicted(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	// empty to start, so shouldn't need eviction
	require.False(t, b.NeedsMutableSegmentsEvicted())

	// perform write and ensure it says it needs eviction
	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(start)
	h1.EXPECT().OnIndexSuccess(start)
	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(time.Hour))
	batch.Append(WriteBatchEntry{
		Timestamp:     start.Add(time.Minute),
		OnIndexSeries: h1,
	}, testDoc1())
	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	require.True(t, b.NeedsMutableSegmentsEvicted())
}

func TestBlockNeedsMutableSegmentsEvictedMutableSegments(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	// empty to start, so shouldn't need eviction
	require.False(t, b.NeedsMutableSegmentsEvicted())
	seg1 := segment.NewMockMutableSegment(ctrl)
	seg1.EXPECT().Size().Return(int64(0)).AnyTimes()
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.NoError(t, b.AddResults(results))
	require.False(t, b.NeedsMutableSegmentsEvicted())

	seg2 := segment.NewMockMutableSegment(ctrl)
	seg2.EXPECT().Size().Return(int64(1)).AnyTimes()
	seg3 := segment.NewMockSegment(ctrl)
	results = result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, true), result.NewSegment(seg3, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 4)))
	require.NoError(t, b.AddResults(results))
	require.True(t, b.NeedsMutableSegmentsEvicted())
}

func TestBlockEvictMutableSegmentsSimple(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	err = blk.EvictMutableSegments()
	require.Error(t, err)

	require.NoError(t, blk.Seal())
	err = blk.EvictMutableSegments()
	require.NoError(t, err)
}

func TestBlockEvictMutableSegmentsAddResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)
	require.NoError(t, b.Seal())

	seg1 := segment.NewMockMutableSegment(ctrl)
	results := result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 3)))
	require.NoError(t, b.AddResults(results))
	seg1.EXPECT().Close().Return(nil)
	err = b.EvictMutableSegments()
	require.NoError(t, err)

	seg2 := segment.NewMockMutableSegment(ctrl)
	seg3 := segment.NewMockSegment(ctrl)
	results = result.NewIndexBlockByVolumeType(start)
	results.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, true), result.NewSegment(seg3, true)},
			result.NewShardTimeRangesFromRange(start, start.Add(time.Hour), 1, 2, 4)))
	require.NoError(t, b.AddResults(results))
	seg2.EXPECT().Close().Return(nil)
	err = b.EvictMutableSegments()
	require.NoError(t, err)
}

func TestBlockE2EInsertQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	testMD := newTestNSMetadata(t)
	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	// Use a larger batch size to simulate large number in a batch
	// coming back (to ensure code path for reusing buffers for iterator
	// is covered).
	testOpts := optionsWithDocsArrayPool(testOpts, 16, 256)

	blk, err := NewBlock(blockStart, testMD,
		BlockOptions{
			ForegroundCompactorMmapDocsData: true,
			BackgroundCompactorMmapDocsData: true,
		},
		namespace.NewRuntimeOptionsManager("foo"),
		testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)
	h2.EXPECT().OnIndexSuccess(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	q, err := idx.NewRegexpQuery([]byte("bar"), []byte("b.*"))
	require.NoError(t, err)

	ctx := context.NewBackground()
	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	queryIter, err := b.QueryIter(ctx, Query{q})
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{}, queryIter, results, time.Now().Add(time.Minute), emptyLogFields)
	require.NoError(t, err)
	require.Equal(t, 2, results.Size())

	rMap := results.Map()
	d, ok := rMap.Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	d, ok = rMap.Get(testDoc2().ID)
	require.True(t, ok)
	t2 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		t2))

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 4)
	require.Equal(t, tracepoint.SearchExecutorIndexSearch, spans[0].OperationName)
	require.Equal(t, tracepoint.NSIdxBlockQueryAddDocuments, spans[1].OperationName)
	require.Equal(t, tracepoint.BlockQuery, spans[2].OperationName)
}

func TestBlockE2EInsertQueryLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().ReconciledOnIndexSeries().Return(h1, &resource.NoopCloser{}, false)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)
	h1.EXPECT().IndexedRange().Return(blockStart, blockStart)
	h1.EXPECT().IndexedForBlockStart(blockStart).Return(true)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)
	h2.EXPECT().OnIndexSuccess(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	q, err := idx.NewRegexpQuery([]byte("bar"), []byte("b.*"))
	require.NoError(t, err)

	limit := 1
	results := NewQueryResults(nil,
		QueryResultsOptions{SizeLimit: limit}, testOpts)
	ctx := context.NewBackground()
	queryIter, err := b.QueryIter(ctx, Query{q})
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{
		SeriesLimit:    limit,
		StartInclusive: blockStart,
		EndExclusive:   blockStart.Add(time.Second),
	}, queryIter, results, time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)
	require.Equal(t, 1, results.Size())

	rMap := results.Map()
	numFound := 0
	d, ok := rMap.Get(testDoc1().ID)
	if ok {
		numFound++
		t1 := test.DocumentToTagIter(t, d)
		require.True(t, ident.NewTagIterMatcher(
			ident.MustNewTagStringsIterator("bar", "baz")).Matches(
			t1))
	}

	d, ok = rMap.Get(testDoc2().ID)
	if ok {
		numFound++
		t2 := test.DocumentToTagIter(t, d)
		require.True(t, ident.NewTagIterMatcher(
			ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
			t2))
	}

	require.Equal(t, 1, numFound)
}

func TestBlockE2EInsertAddResultsQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	closer := &resource.NoopCloser{}
	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().ReconciledOnIndexSeries().Return(h1, closer, false)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)
	h1.EXPECT().IndexedRange().Return(blockStart, blockStart)
	h1.EXPECT().IndexedForBlockStart(blockStart).Return(true)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().ReconciledOnIndexSeries().Return(h2, closer, false)
	h2.EXPECT().OnIndexFinalize(blockStart)
	h2.EXPECT().OnIndexSuccess(blockStart)
	h2.EXPECT().IndexedRange().Return(blockStart, blockStart)
	h2.EXPECT().IndexedForBlockStart(blockStart).Return(true)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	seg := testSegment(t, testDoc1DupeID())
	idxResults := result.NewIndexBlockByVolumeType(blockStart)
	idxResults.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg, true)},
			result.NewShardTimeRangesFromRange(blockStart, blockStart.Add(blockSize), 1, 2, 3)))
	require.NoError(t, blk.AddResults(idxResults))

	q, err := idx.NewRegexpQuery([]byte("bar"), []byte("b.*"))
	require.NoError(t, err)

	ctx := context.NewBackground()
	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	queryIter, err := b.QueryIter(ctx, Query{q})
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{
		StartInclusive: blockStart,
		EndExclusive:   blockStart.Add(time.Second),
	}, queryIter, results, time.Now().Add(time.Minute), emptyLogFields)
	require.NoError(t, err)
	require.Equal(t, 2, results.Size())

	rMap := results.Map()
	d, ok := rMap.Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	d, ok = rMap.Get(testDoc2().ID)
	require.True(t, ok)
	t2 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		t2))

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 6)
	require.Equal(t, tracepoint.SearchExecutorIndexSearch, spans[0].OperationName)
	require.Equal(t, tracepoint.SearchExecutorIndexSearch, spans[1].OperationName)
	require.Equal(t, tracepoint.NSIdxBlockQueryAddDocuments, spans[2].OperationName)
	require.Equal(t, tracepoint.NSIdxBlockQueryAddDocuments, spans[3].OperationName)
	require.Equal(t, tracepoint.BlockQuery, spans[4].OperationName)
}

func TestBlockE2EInsertAddResultsMergeQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().ReconciledOnIndexSeries().Return(h1, &resource.NoopCloser{}, false)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)
	h1.EXPECT().IndexedRange().Return(blockStart, blockStart)
	h1.EXPECT().IndexedForBlockStart(blockStart).Return(true)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	seg := testSegment(t, testDoc2())
	idxResults := result.NewIndexBlockByVolumeType(blockStart)
	idxResults.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg, true)},
			result.NewShardTimeRangesFromRange(blockStart, blockStart.Add(blockSize), 1, 2, 3)))
	require.NoError(t, blk.AddResults(idxResults))

	q, err := idx.NewRegexpQuery([]byte("bar"), []byte("b.*"))
	require.NoError(t, err)

	ctx := context.NewBackground()
	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	queryIter, err := b.QueryIter(ctx, Query{q})
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{
		StartInclusive: blockStart,
		EndExclusive:   blockStart.Add(time.Second),
	}, queryIter, results, time.Now().Add(time.Minute), emptyLogFields)
	require.NoError(t, err)
	require.Equal(t, 2, results.Size())

	rMap := results.Map()
	d, ok := results.Map().Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	d, ok = rMap.Get(testDoc2().ID)
	require.True(t, ok)
	t2 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		t2))

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 6)
	require.Equal(t, tracepoint.SearchExecutorIndexSearch, spans[0].OperationName)
	require.Equal(t, tracepoint.SearchExecutorIndexSearch, spans[1].OperationName)
	require.Equal(t, tracepoint.NSIdxBlockQueryAddDocuments, spans[2].OperationName)
	require.Equal(t, tracepoint.NSIdxBlockQueryAddDocuments, spans[3].OperationName)
	require.Equal(t, tracepoint.BlockQuery, spans[4].OperationName)
}

func TestBlockE2EInsertAddResultsQueryNarrowingBlockRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	closer := &resource.NoopCloser{}
	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().ReconciledOnIndexSeries().Return(h1, closer, false)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)
	h1.EXPECT().IndexedRange().Return(blockStart, blockStart.Add(2*blockSize))
	h1.EXPECT().IndexedForBlockStart(blockStart).Return(false)
	h1.EXPECT().IndexedForBlockStart(blockStart.Add(1 * blockSize)).Return(false)
	h1.EXPECT().IndexedForBlockStart(blockStart.Add(2 * blockSize)).Return(true)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().ReconciledOnIndexSeries().Return(h2, closer, false)
	h2.EXPECT().OnIndexFinalize(blockStart)
	h2.EXPECT().OnIndexSuccess(blockStart)
	h2.EXPECT().IndexedRange().Return(xtime.UnixNano(0), xtime.UnixNano(0))

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	seg := testSegment(t, testDoc1DupeID())
	idxResults := result.NewIndexBlockByVolumeType(blockStart)
	idxResults.SetBlock(idxpersist.DefaultIndexVolumeType,
		result.NewIndexBlock([]result.Segment{result.NewSegment(seg, true)},
			result.NewShardTimeRangesFromRange(blockStart, blockStart.Add(blockSize), 1, 2, 3)))
	require.NoError(t, blk.AddResults(idxResults))

	q, err := idx.NewRegexpQuery([]byte("bar"), []byte("b.*"))
	require.NoError(t, err)

	ctx := context.NewBackground()
	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	results := NewQueryResults(nil, QueryResultsOptions{}, testOpts)
	queryIter, err := b.QueryIter(ctx, Query{q})
	require.NoError(t, err)
	err = b.QueryWithIter(ctx, QueryOptions{
		StartInclusive: blockStart.Add(-1000 * blockSize),
		EndExclusive:   blockStart.Add(1000 * blockSize),
	}, queryIter, results, time.Now().Add(time.Minute), emptyLogFields)
	require.NoError(t, err)
	require.Equal(t, 1, results.Size())

	rMap := results.Map()
	d, ok := rMap.Get(testDoc1().ID)
	require.True(t, ok)
	t1 := test.DocumentToTagIter(t, d)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		t1))

	_, ok = rMap.Get(testDoc2().ID)
	require.False(t, ok)

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 5)
	require.Equal(t, tracepoint.SearchExecutorIndexSearch, spans[0].OperationName)
	require.Equal(t, tracepoint.SearchExecutorIndexSearch, spans[1].OperationName)
	require.Equal(t, tracepoint.NSIdxBlockQueryAddDocuments, spans[2].OperationName)
	require.Equal(t, tracepoint.BlockQuery, spans[3].OperationName)
}

func TestBlockWriteBackgroundCompact(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	testOpts = testOpts.SetInstrumentOptions(
		testOpts.InstrumentOptions().SetLogger(logger))

	blk, err := NewBlock(blockStart, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, blk.Close())
	}()

	b, ok := blk.(*block)
	require.True(t, ok)

	// Testing compaction only, so mark GC as already running so the test is limited only to compaction.
	b.mutableSegments.compact.compactingBackgroundGarbageCollect = true

	// First write
	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)
	h2.EXPECT().OnIndexSuccess(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	// Move the segment to background
	b.Lock()
	b.mutableSegments.maybeMoveForegroundSegmentsToBackgroundWithLock([]compaction.Segment{
		{Segment: b.mutableSegments.foregroundSegments[0].Segment()},
	})
	b.Unlock()

	// Second write
	h1 = doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	batch = NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc3())

	res, err = b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	// Move last segment to background, this should kick off a background compaction
	b.mutableSegments.Lock()
	b.mutableSegments.maybeMoveForegroundSegmentsToBackgroundWithLock([]compaction.Segment{
		{Segment: b.mutableSegments.foregroundSegments[0].Segment()},
	})
	require.Equal(t, 2, len(b.mutableSegments.backgroundSegments))
	require.True(t, b.mutableSegments.compact.compactingBackgroundStandard)
	b.mutableSegments.Unlock()

	// Wait for compaction to finish
	for {
		b.mutableSegments.RLock()
		compacting := b.mutableSegments.compact.compactingBackgroundStandard
		b.mutableSegments.RUnlock()
		if !compacting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Make sure compacted into a single segment
	b.mutableSegments.RLock()
	require.Equal(t, 1, len(b.mutableSegments.backgroundSegments))
	require.Equal(t, 3, int(b.mutableSegments.backgroundSegments[0].Segment().Size()))
	b.mutableSegments.RUnlock()
}

func TestBlockAggregateAfterClose(t *testing.T) {
	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	b, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	require.Equal(t, start, b.StartTime())
	require.Equal(t, start.Add(time.Hour), b.EndTime())
	require.NoError(t, b.Close())

	_, err = b.AggregateIter(context.NewBackground(), AggregateResultsOptions{})
	require.Error(t, err)
}

func TestBlockAggregateIterationErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	reader := segment.NewMockReader(ctrl)
	reader.EXPECT().Close().Return(nil)
	seg1.EXPECT().Reader().Return(reader, nil)

	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, testOpts)}
	iter := NewMockfieldsAndTermsIterator(ctrl)
	b.newFieldsAndTermsIteratorFn = func(
		_ context.Context, _ segment.Reader, opts fieldsAndTermsIteratorOpts) (fieldsAndTermsIterator, error) {
		return iter, nil
	}

	results := NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
		SizeLimit: 3,
		Type:      AggregateTagNamesAndValues,
	}, testOpts)

	gomock.InOrder(
		iter.EXPECT().Next().Return(true),
		iter.EXPECT().Current().Return([]byte("f1"), []byte("t1")),
		iter.EXPECT().Next().Return(false),
		iter.EXPECT().Err().Return(fmt.Errorf("unknown error")),
	)

	ctx := context.NewBackground()
	defer ctx.BlockingClose()

	aggIter, err := b.AggregateIter(ctx, results.AggregateResultsOptions())
	require.NoError(t, err)
	err = b.AggregateWithIter(
		ctx,
		aggIter,
		QueryOptions{SeriesLimit: 3},
		results,
		time.Now().Add(time.Minute),
		emptyLogFields)
	require.Error(t, err)
}

func TestBlockAggregate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)
	limitOpts := limits.NewOptions().
		SetInstrumentOptions(iOpts).
		SetDocsLimitOpts(limits.LookbackLimitOptions{Limit: 50, Lookback: time.Minute}).
		SetBytesReadLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute}).
		SetAggregateDocsLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute})
	queryLimits, err := limits.NewQueryLimits(limitOpts)
	require.NoError(t, err)
	testOpts = testOpts.SetInstrumentOptions(iOpts).SetQueryLimits(queryLimits)

	// NB: seriesLimit must be higher than the number of fields to be exhaustive.
	seriesLimit := 10
	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	reader := segment.NewMockReader(ctrl)
	reader.EXPECT().Close().Return(nil)
	seg1.EXPECT().Reader().Return(reader, nil)

	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, testOpts)}
	iter := NewMockfieldsAndTermsIterator(ctrl)
	b.newFieldsAndTermsIteratorFn = func(
		_ context.Context, _ segment.Reader, opts fieldsAndTermsIteratorOpts) (fieldsAndTermsIterator, error) {
		return iter, nil
	}

	results := NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
		SizeLimit: seriesLimit,
		Type:      AggregateTagNamesAndValues,
	}, testOpts)

	ctx := context.NewBackground()
	defer ctx.BlockingClose()

	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	iter.EXPECT().Next().Return(true)
	iter.EXPECT().Current().Return([]byte("f1"), []byte("t1"))
	iter.EXPECT().Next().Return(true)
	iter.EXPECT().Current().Return([]byte("f1"), []byte("t2"))
	iter.EXPECT().Next().Return(true)
	iter.EXPECT().Current().Return([]byte("f2"), []byte("t1"))
	iter.EXPECT().Next().Return(true)
	iter.EXPECT().Current().Return([]byte("f1"), []byte("t3"))
	iter.EXPECT().Next().Return(false)
	iter.EXPECT().Err().Return(nil)
	iter.EXPECT().Close().Return(nil)

	aggIter, err := b.AggregateIter(ctx, results.AggregateResultsOptions())
	require.NoError(t, err)
	err = b.AggregateWithIter(
		ctx,
		aggIter,
		QueryOptions{SeriesLimit: seriesLimit},
		results,
		time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	assertAggregateResultsMapEquals(t, map[string][]string{
		"f1": {"t1", "t2", "t3"},
		"f2": {"t1"},
	}, results)

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 3)
	require.Equal(t, tracepoint.NSIdxBlockAggregateQueryAddDocuments, spans[0].OperationName)
	require.Equal(t, tracepoint.BlockAggregate, spans[1].OperationName)

	snap := scope.Snapshot()

	tallytest.AssertCounterValue(t, 3, snap,
		"query-limit.total-docs-matched", map[string]string{"type": "fetch"})
	tallytest.AssertCounterValue(t, 7, snap,
		"query-limit.total-docs-matched", map[string]string{"type": "aggregate"})
}

func TestBlockAggregateWithAggregateLimits(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	seriesLimit := 100
	scope := tally.NewTestScope("", nil)
	iOpts := instrument.NewOptions().SetMetricsScope(scope)
	limitOpts := limits.NewOptions().
		SetInstrumentOptions(iOpts).
		SetDocsLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute}).
		SetBytesReadLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute}).
		SetAggregateDocsLimitOpts(limits.LookbackLimitOptions{
			Limit:    int64(seriesLimit),
			Lookback: time.Minute,
		})
	queryLimits, err := limits.NewQueryLimits(limitOpts)
	require.NoError(t, err)
	aggTestOpts := testOpts.SetInstrumentOptions(iOpts).SetQueryLimits(queryLimits)

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), aggTestOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	reader := segment.NewMockReader(ctrl)
	reader.EXPECT().Close().Return(nil)
	seg1.EXPECT().Reader().Return(reader, nil)

	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, aggTestOpts)}
	iter := NewMockfieldsAndTermsIterator(ctrl)
	b.newFieldsAndTermsIteratorFn = func(
		_ context.Context, _ segment.Reader, opts fieldsAndTermsIteratorOpts) (fieldsAndTermsIterator, error) {
		return iter, nil
	}
	results := NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
		SizeLimit: seriesLimit,
		Type:      AggregateTagNamesAndValues,
	}, aggTestOpts)

	ctx := context.NewBackground()
	defer ctx.BlockingClose()

	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	// use seriesLimit instead of seriesLimit - 1 since the iterator peeks ahead to check for Done.
	for i := 0; i < seriesLimit; i++ {
		iter.EXPECT().Next().Return(true)
		curr := []byte(fmt.Sprint(i))
		iter.EXPECT().Current().Return([]byte("f1"), curr)
	}

	aggIter, err := b.AggregateIter(ctx, results.AggregateResultsOptions())
	require.NoError(t, err)
	err = b.AggregateWithIter(
		ctx,
		aggIter,
		QueryOptions{SeriesLimit: seriesLimit},
		results,
		time.Now().Add(time.Minute),
		emptyLogFields)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "query aborted due to limit"))

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 3)
	require.Equal(t, tracepoint.NSIdxBlockAggregateQueryAddDocuments, spans[0].OperationName)
	require.Equal(t, tracepoint.BlockAggregate, spans[1].OperationName)

	snap := scope.Snapshot()
	tallytest.AssertCounterValue(t, 1, snap,
		"query-limit.total-docs-matched", map[string]string{"type": "fetch"})
	tallytest.AssertCounterValue(t, int64(seriesLimit), snap,
		"query-limit.total-docs-matched", map[string]string{"type": "aggregate"})
}

func TestBlockAggregateNotExhaustive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testMD := newTestNSMetadata(t)
	start := xtime.Now().Truncate(time.Hour)

	aggResultsEntryArrayPool := NewAggregateResultsEntryArrayPool(AggregateResultsEntryArrayPoolOpts{
		Options: pool.NewObjectPoolOptions().
			SetSize(aggregateResultsEntryArrayPoolSize),
		Capacity:    1,
		MaxCapacity: 1,
	})
	aggResultsEntryArrayPool.Init()
	opts := testOpts.SetAggregateResultsEntryArrayPool(aggResultsEntryArrayPool)

	blk, err := NewBlock(start, testMD, BlockOptions{},
		namespace.NewRuntimeOptionsManager("foo"), opts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	reader := segment.NewMockReader(ctrl)
	reader.EXPECT().Close().Return(nil)
	seg1.EXPECT().Reader().Return(reader, nil)

	b.mutableSegments.foregroundSegments = []*readableSeg{newReadableSeg(seg1, testOpts)}
	iter := NewMockfieldsAndTermsIterator(ctrl)
	b.newFieldsAndTermsIteratorFn = func(
		_ context.Context, _ segment.Reader, opts fieldsAndTermsIteratorOpts) (fieldsAndTermsIterator, error) {
		return iter, nil
	}

	results := NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
		SizeLimit: 1,
		Type:      AggregateTagNamesAndValues,
	}, testOpts)

	ctx := context.NewBackground()
	defer ctx.BlockingClose()

	// create initial span from a mock tracer and get ctx
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	gomock.InOrder(
		iter.EXPECT().Next().Return(true),
		iter.EXPECT().Current().Return([]byte("f1"), []byte("t1")),
		iter.EXPECT().Next().Return(true),
		// even though there is a limit 1, the iterator peeks ahead so 3 results are actually consumed.
		iter.EXPECT().Current().Return([]byte("f2"), []byte("t2")),
		iter.EXPECT().Next().Return(true),
		iter.EXPECT().Current().Return([]byte("f3"), []byte("f3")),
	)
	aggIter, err := b.AggregateIter(ctx, results.AggregateResultsOptions())
	require.NoError(t, err)
	err = b.AggregateWithIter(
		ctx,
		aggIter,
		QueryOptions{SeriesLimit: 1},
		results,
		time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)

	assertAggregateResultsMapEquals(t, map[string][]string{
		"f1": {},
	}, results)

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 3)
	require.Equal(t, tracepoint.NSIdxBlockAggregateQueryAddDocuments, spans[0].OperationName)
	require.Equal(t, tracepoint.BlockAggregate, spans[1].OperationName)
}

func TestBlockE2EInsertAggregate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	testMD := newTestNSMetadata(t)
	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	// Use a larger batch size to simulate large number in a batch
	// coming back (to ensure code path for reusing buffers for iterator
	// is covered).
	testOpts := optionsWithDocsArrayPool(testOpts, 16, 256)

	blk, err := NewBlock(blockStart, testMD,
		BlockOptions{
			ForegroundCompactorMmapDocsData: true,
			BackgroundCompactorMmapDocsData: true,
		},
		namespace.NewRuntimeOptionsManager("foo"),
		testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)
	h2.EXPECT().OnIndexSuccess(blockStart)

	h3 := doc.NewMockOnIndexSeries(ctrl)
	h3.EXPECT().OnIndexFinalize(blockStart)
	h3.EXPECT().OnIndexSuccess(blockStart)

	batch := NewWriteBatch(testWriteBatchOptionsWithBlockSize(blockSize))
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h3,
	}, testDoc3())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(3), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	results := NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
		SizeLimit: 10,
		Type:      AggregateTagNamesAndValues,
	}, testOpts)

	ctx := context.NewBackground()
	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

	aggIter, err := b.AggregateIter(ctx, results.AggregateResultsOptions())
	require.NoError(t, err)
	err = b.AggregateWithIter(
		ctx,
		aggIter,
		QueryOptions{SeriesLimit: 1000},
		results,
		time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)
	assertAggregateResultsMapEquals(t, map[string][]string{
		"bar":  {"baz", "qux"},
		"some": {"more", "other"},
	}, results)

	results = NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
		SizeLimit:   10,
		Type:        AggregateTagNamesAndValues,
		FieldFilter: AggregateFieldFilter{[]byte("bar")},
	}, testOpts)
	aggIter, err = b.AggregateIter(ctx, results.AggregateResultsOptions())
	require.NoError(t, err)
	err = b.AggregateWithIter(
		ctx,
		aggIter,
		QueryOptions{SeriesLimit: 1000},
		results,
		time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)
	assertAggregateResultsMapEquals(t, map[string][]string{
		"bar": {"baz", "qux"},
	}, results)

	results = NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
		SizeLimit:   10,
		Type:        AggregateTagNamesAndValues,
		FieldFilter: AggregateFieldFilter{[]byte("random")},
	}, testOpts)
	aggIter, err = b.AggregateIter(ctx, results.AggregateResultsOptions())
	require.NoError(t, err)
	err = b.AggregateWithIter(
		ctx,
		aggIter,
		QueryOptions{SeriesLimit: 1000},
		results,
		time.Now().Add(time.Minute),
		emptyLogFields)
	require.NoError(t, err)
	assertAggregateResultsMapEquals(t, map[string][]string{}, results)

	sp.Finish()
	spans := mtr.FinishedSpans()
	require.Len(t, spans, 6)
	require.Equal(t, tracepoint.NSIdxBlockAggregateQueryAddDocuments, spans[0].OperationName)
	require.Equal(t, tracepoint.BlockAggregate, spans[1].OperationName)
	require.Equal(t, tracepoint.NSIdxBlockAggregateQueryAddDocuments, spans[2].OperationName)
	require.Equal(t, tracepoint.BlockAggregate, spans[3].OperationName)
	require.Equal(t, tracepoint.BlockAggregate, spans[4].OperationName)
}

func assertAggregateResultsMapEquals(t *testing.T, expected map[string][]string, observed AggregateResults) {
	aggResultsMap := observed.Map()
	// ensure `expected` contained in `observed`
	for field, terms := range expected {
		entry, ok := aggResultsMap.Get(ident.StringID(field))
		require.True(t, ok,
			fmt.Sprintf("field from expected map missing in observed: field=%s", field))
		valuesMap := entry.valuesMap
		for _, term := range terms {
			_, ok = valuesMap.Get(ident.StringID(term))
			require.True(t, ok,
				fmt.Sprintf("term from expected map missing in observed: field=%s, term=%s", field, term))
		}
	}
	// ensure `observed` contained in `expected`
	for _, entry := range aggResultsMap.Iter() {
		field := entry.Key()
		valuesMap := entry.Value().valuesMap
		for _, entry := range valuesMap.Iter() {
			term := entry.Key()
			slice, ok := expected[field.String()]
			require.True(t, ok,
				fmt.Sprintf("field from observed map missing in expected: field=%s", field.String()))
			found := false
			for _, expTerm := range slice {
				if expTerm == term.String() {
					found = true
				}
			}
			require.True(t, found,
				fmt.Sprintf("term from observed map missing in expected: field=%s, term=%s", field.String(), term.String()))
		}
	}
}

func testSegment(t *testing.T, docs ...doc.Metadata) segment.Segment {
	seg, err := mem.NewSegment(testOpts.MemSegmentOptions())
	require.NoError(t, err)

	for _, d := range docs {
		_, err = seg.Insert(d)
		require.NoError(t, err)
	}

	return seg
}

func testDoc1() doc.Metadata {
	return doc.Metadata{
		ID: []byte("foo"),
		Fields: []doc.Field{
			{
				Name:  []byte("bar"),
				Value: []byte("baz"),
			},
		},
	}
}

func testDoc1DupeID() doc.Metadata {
	return doc.Metadata{
		ID: []byte("foo"),
		Fields: []doc.Field{
			{
				Name:  []byte("why"),
				Value: []byte("not"),
			},
			{
				Name:  []byte("some"),
				Value: []byte("more"),
			},
		},
	}
}

func testDoc2() doc.Metadata {
	return doc.Metadata{
		ID: []byte("something"),
		Fields: []doc.Field{
			{
				Name:  []byte("bar"),
				Value: []byte("baz"),
			},
			{
				Name:  []byte("some"),
				Value: []byte("more"),
			},
		},
	}
}

func testDoc3() doc.Metadata {
	return doc.Metadata{
		ID: []byte("bar"),
		Fields: []doc.Field{
			{
				Name:  []byte("bar"),
				Value: []byte("qux"),
			},
			{
				Name:  []byte("some"),
				Value: []byte("other"),
			},
		},
	}
}

func optionsWithAggResultsPool(capacity int) Options {
	pool := NewAggregateResultsEntryArrayPool(
		AggregateResultsEntryArrayPoolOpts{
			Capacity: capacity,
		},
	)

	pool.Init()
	return testOpts.SetAggregateResultsEntryArrayPool(pool)
}

func buildSegment(t *testing.T, term string, fields []string, opts mem.Options) *readableSeg {
	seg, err := mem.NewSegment(opts)
	require.NoError(t, err)

	docFields := make([]doc.Field, 0, len(fields))
	sort.Strings(fields)
	for _, field := range fields {
		docFields = append(docFields, doc.Field{Name: []byte(term), Value: []byte(field)})
	}

	_, err = seg.Insert(doc.Metadata{Fields: docFields})
	require.NoError(t, err)

	require.NoError(t, seg.Seal())
	return newReadableSeg(seg, testOpts)
}

func TestBlockAggregateBatching(t *testing.T) {
	memOpts := mem.NewOptions()

	var (
		batchSizeMap      = make(map[string][]string)
		batchSizeSegments = make([]*readableSeg, 0, defaultQueryDocsBatchSize)
	)

	for i := 0; i < defaultQueryDocsBatchSize; i++ {
		fields := make([]string, 0, defaultQueryDocsBatchSize)
		for j := 0; j < defaultQueryDocsBatchSize; j++ {
			fields = append(fields, fmt.Sprintf("bar_%d", j))
		}

		if i == 0 {
			batchSizeMap["foo"] = fields
		}

		batchSizeSegments = append(batchSizeSegments, buildSegment(t, "foo", fields, memOpts))
	}

	tests := []struct {
		name                   string
		batchSize              int
		segments               []*readableSeg
		expectedDocsMatched    int64
		expectedAggDocsMatched int64
		expected               map[string][]string
	}{
		{
			name:      "single term multiple fields duplicated across readers",
			batchSize: 3,
			segments: []*readableSeg{
				buildSegment(t, "foo", []string{"bar", "baz"}, memOpts),
				buildSegment(t, "foo", []string{"bar", "baz"}, memOpts),
				buildSegment(t, "foo", []string{"bar", "baz"}, memOpts),
			},
			expectedDocsMatched:    1,
			expectedAggDocsMatched: 9,
			expected: map[string][]string{
				"foo": {"bar", "baz"},
			},
		},
		{
			name:      "multiple term multiple fields",
			batchSize: 3,
			segments: []*readableSeg{
				buildSegment(t, "foo", []string{"bar", "baz"}, memOpts),
				buildSegment(t, "foo", []string{"bag", "bat"}, memOpts),
				buildSegment(t, "qux", []string{"bar", "baz"}, memOpts),
			},
			expectedDocsMatched:    2,
			expectedAggDocsMatched: 9,
			expected: map[string][]string{
				"foo": {"bag", "bar", "bat", "baz"},
				"qux": {"bar", "baz"},
			},
		},
		{
			name: "term present in first and third reader",
			// NB: expecting three batches due to the way batches are split (on the
			// first different term ID in a batch), will look like this:
			// [{foo [bar baz]} {dog [bar baz]} {qux [bar]}
			// [{qux [baz]} {qaz [bar baz]} {foo [bar]}]
			// [{foo [baz]}]
			batchSize: 3,
			segments: []*readableSeg{
				buildSegment(t, "foo", []string{"bar", "baz"}, memOpts),
				buildSegment(t, "dog", []string{"bar", "baz"}, memOpts),
				buildSegment(t, "qux", []string{"bar", "baz"}, memOpts),
				buildSegment(t, "qaz", []string{"bar", "baz"}, memOpts),
				buildSegment(t, "foo", []string{"bar", "baz"}, memOpts),
			},
			expectedDocsMatched:    7,
			expectedAggDocsMatched: 15,
			expected: map[string][]string{
				"foo": {"bar", "baz"},
				"dog": {"bar", "baz"},
				"qux": {"bar", "baz"},
				"qaz": {"bar", "baz"},
			},
		},
		{
			name:                   "batch size case",
			batchSize:              defaultQueryDocsBatchSize,
			segments:               batchSizeSegments,
			expectedDocsMatched:    1,
			expectedAggDocsMatched: 256*257 + 2,
			expected:               batchSizeMap,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope := tally.NewTestScope("", nil)
			iOpts := instrument.NewOptions().SetMetricsScope(scope)
			limitOpts := limits.NewOptions().
				SetInstrumentOptions(iOpts).
				SetDocsLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute}).
				SetBytesReadLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute}).
				SetAggregateDocsLimitOpts(limits.LookbackLimitOptions{Lookback: time.Minute})
			queryLimits, err := limits.NewQueryLimits(limitOpts)
			require.NoError(t, err)
			testOpts = optionsWithAggResultsPool(tt.batchSize).
				SetInstrumentOptions(iOpts).
				SetQueryLimits(queryLimits)

			testMD := newTestNSMetadata(t)
			start := xtime.Now().Truncate(time.Hour)
			blk, err := NewBlock(start, testMD, BlockOptions{},
				namespace.NewRuntimeOptionsManager("foo"), testOpts)
			require.NoError(t, err)

			b, ok := blk.(*block)
			require.True(t, ok)

			// NB: wrap existing aggregate results fn to more easily inspect batch size.
			addAggregateResultsFn := b.addAggregateResultsFn
			b.addAggregateResultsFn = func(
				ctx context.Context,
				results AggregateResults,
				batch []AggregateResultsEntry,
				source []byte,
			) ([]AggregateResultsEntry, int, int, error) {
				// NB: since both terms and values count towards the batch size, initialize
				// this with batch size to account for terms.
				count := len(batch)
				for _, entry := range batch {
					count += len(entry.Terms)
				}

				require.True(t, count <= tt.batchSize,
					fmt.Sprintf("batch %v exceeds batchSize %d", batch, tt.batchSize))

				return addAggregateResultsFn(ctx, results, batch, source)
			}

			b.mutableSegments.foregroundSegments = tt.segments
			results := NewAggregateResults(ident.StringID("ns"), AggregateResultsOptions{
				Type: AggregateTagNamesAndValues,
			}, testOpts)

			ctx := context.NewBackground()
			defer ctx.BlockingClose()

			aggIter, err := b.AggregateIter(ctx, results.AggregateResultsOptions())
			require.NoError(t, err)
			err = b.AggregateWithIter(
				ctx,
				aggIter,
				QueryOptions{},
				results,
				time.Now().Add(time.Minute),
				emptyLogFields)
			require.NoError(t, err)

			snap := scope.Snapshot()
			tallytest.AssertCounterValue(t, tt.expectedDocsMatched, snap,
				"query-limit.total-docs-matched", map[string]string{"type": "fetch"})
			tallytest.AssertCounterValue(t, tt.expectedAggDocsMatched, snap,
				"query-limit.total-docs-matched", map[string]string{"type": "aggregate"})

			resultsMap := make(map[string][]string, results.Map().Len())
			for _, res := range results.Map().Iter() {
				vals := make([]string, 0, res.Value().valuesMap.Len())
				for _, val := range res.Value().valuesMap.Iter() {
					vals = append(vals, val.Key().String())
				}

				sort.Strings(vals)
				resultsMap[res.Key().String()] = vals
			}

			assert.Equal(t, tt.expected, resultsMap)
		})
	}
}
