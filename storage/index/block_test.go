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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/idx"
	"github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBlockCtor(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	b, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	require.Equal(t, start, b.StartTime())
	require.Equal(t, start.Add(time.Hour), b.EndTime())
	require.NoError(t, b.Close())
	require.Error(t, b.Close())
}

func TestBlockWriteAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	b, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	require.NoError(t, b.Close())

	lifecycle := NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: lifecycle,
	}, doc.Document{})

	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(0), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Document,
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

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	b, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	require.NoError(t, b.Seal())

	lifecycle := NewMockOnIndexSeries(ctrl)
	lifecycle.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: lifecycle,
	}, doc.Document{})

	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(0), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Document,
		result WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.Equal(t, errUnableToWriteBlockSealed, result.Err)
	})
	require.Equal(t, 1, verified)
}

func TestBlockWriteMockSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h1.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	h2 := NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h2.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	seg := segment.NewMockMutableSegment(ctrl)
	b.segment = seg
	seg.EXPECT().InsertBatch(index.NewBatchMatcher(
		index.Batch{
			Docs:                []doc.Document{testDoc1(), testDoc1DupeID()},
			AllowPartialUpdates: true,
		},
	)).Return(nil)

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc1DupeID())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(2), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)
}

func TestBlockWriteActualSegmentPartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h1.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	h2 := NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc1DupeID())
	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Document,
		result WriteBatchEntryResult,
	) {
		verified++
		if idx == 0 {
			require.NoError(t, result.Err)
		} else {
			require.Error(t, result.Err)
			require.Equal(t, mem.ErrDuplicateID, result.Err)
		}
	})
	require.Equal(t, 2, verified)
}

func TestBlockWriteMockSegmentPartialFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	seg := segment.NewMockMutableSegment(ctrl)
	b.segment = seg

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h1.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	h2 := NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))

	testErr := fmt.Errorf("random-err")

	berr := index.NewBatchPartialError()
	berr.Add(index.BatchError{testErr, 1})
	seg.EXPECT().InsertBatch(index.NewBatchMatcher(
		index.Batch{
			Docs:                []doc.Document{testDoc1(), testDoc1DupeID()},
			AllowPartialUpdates: true,
		},
	)).Return(berr)

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc1DupeID())

	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(1), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Document,
		result WriteBatchEntryResult,
	) {
		verified++
		if idx == 0 {
			require.NoError(t, result.Err)
		} else {
			require.Error(t, result.Err)
			require.Equal(t, testErr, result.Err)
		}
	})
	require.Equal(t, 2, verified)
}

func TestBlockWriteMockSegmentUnexpectedErrorType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	seg := segment.NewMockMutableSegment(ctrl)
	b.segment = seg

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))

	h2 := NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))

	testErr := fmt.Errorf("random-err")

	seg.EXPECT().InsertBatch(index.NewBatchMatcher(
		index.Batch{
			Docs:                []doc.Document{testDoc1(), testDoc1DupeID()},
			AllowPartialUpdates: true,
		},
	)).Return(testErr)

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc1DupeID())

	res, err := b.WriteBatch(batch)
	require.Error(t, err)
	require.Equal(t, int64(2), res.NumError)

	verified := 0
	batch.ForEach(func(
		idx int,
		entry WriteBatchEntry,
		doc doc.Document,
		result WriteBatchEntryResult,
	) {
		verified++
		require.Error(t, result.Err)
		require.True(t, strings.Contains(result.Err.Error(), "unexpected non-BatchPartialError"))
	})
	require.Equal(t, 2, verified)
}

func TestBlockQueryAfterClose(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	b, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	require.Equal(t, start, b.StartTime())
	require.Equal(t, start.Add(time.Hour), b.EndTime())
	require.NoError(t, b.Close())

	_, err = b.Query(Query{}, QueryOptions{}, nil)
	require.Error(t, err)
}

func TestBlockQueryExecutorError(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	b.newExecutorFn = func() (search.Executor, error) {
		b.RLock() // ensures we call newExecutorFn with RLock, or this would deadlock
		defer b.RUnlock()
		return nil, fmt.Errorf("random-err")
	}

	_, err = b.Query(Query{}, QueryOptions{}, nil)
	require.Error(t, err)
}

func TestBlockQuerySegmentReaderError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg := segment.NewMockMutableSegment(ctrl)
	b.segment = seg
	randErr := fmt.Errorf("random-err")
	seg.EXPECT().Reader().Return(nil, randErr)

	_, err = b.Query(Query{}, QueryOptions{}, nil)
	require.Equal(t, randErr, err)
}

func TestBlockQueryBootstrapSegmentsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	seg2 := segment.NewMockMutableSegment(ctrl)
	seg3 := segment.NewMockMutableSegment(ctrl)

	b.segment = seg1
	b.bootstrappedSegments = append(b.bootstrappedSegments, seg2, seg3)

	r1 := index.NewMockReader(ctrl)
	seg1.EXPECT().Reader().Return(r1, nil)
	r1.EXPECT().Close().Return(nil)

	r2 := index.NewMockReader(ctrl)
	seg2.EXPECT().Reader().Return(r2, nil)
	r2.EXPECT().Close().Return(nil)

	randErr := fmt.Errorf("random-err")
	seg3.EXPECT().Reader().Return(nil, randErr)

	_, err = b.Query(Query{}, QueryOptions{}, nil)
	require.Equal(t, randErr, err)
}

func TestBlockMockQueryExecutorExecError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	// dIter:= doc.NewMockIterator(ctrl)
	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(nil, fmt.Errorf("randomerr")),
		exec.EXPECT().Close(),
	)
	_, err = b.Query(Query{}, QueryOptions{}, nil)
	require.Error(t, err)
}

func TestBlockMockQueryExecutorExecIterErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(testDoc1()),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(fmt.Errorf("randomerr")),
		dIter.EXPECT().Close(),
		exec.EXPECT().Close(),
	)
	_, err = b.Query(Query{}, QueryOptions{}, NewResults(testOpts))
	require.Error(t, err)
}

func TestBlockMockQueryExecutorExecLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(testDoc1()),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Close().Return(nil),
		exec.EXPECT().Close().Return(nil),
	)
	results := NewResults(testOpts)
	exhaustive, err := b.Query(Query{}, QueryOptions{Limit: 1}, results)
	require.NoError(t, err)
	require.False(t, exhaustive)

	rMap := results.Map()
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))
}

func TestBlockMockQueryExecutorExecIterCloseErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Close().Return(fmt.Errorf("random-err")),
		exec.EXPECT().Close().Return(nil),
	)
	results := NewResults(testOpts)
	_, err = b.Query(Query{}, QueryOptions{}, results)
	require.Error(t, err)
}

func TestBlockMockQueryExecutorExecIterExecCloseErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Close().Return(nil),
		exec.EXPECT().Close().Return(fmt.Errorf("randomerr")),
	)
	results := NewResults(testOpts)
	_, err = b.Query(Query{}, QueryOptions{}, results)
	require.Error(t, err)
}

func TestBlockMockQueryLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(testDoc1()),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Close().Return(nil),
		exec.EXPECT().Close().Return(nil),
	)
	results := NewResults(testOpts)
	exhaustive, err := b.Query(Query{}, QueryOptions{Limit: 1}, results)
	require.NoError(t, err)
	require.False(t, exhaustive)

	rMap := results.Map()
	require.Equal(t, 1, rMap.Len())
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))
}

func TestBlockMockQueryLimitExhaustive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(testDoc1()),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Close().Return(nil),
		exec.EXPECT().Close().Return(nil),
	)
	results := NewResults(testOpts)
	exhaustive, err := b.Query(Query{}, QueryOptions{Limit: 1}, results)
	require.NoError(t, err)
	require.True(t, exhaustive)

	rMap := results.Map()
	require.Equal(t, 1, rMap.Len())
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))
}

func TestBlockMockQueryMergeResultsMapLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)
	require.NoError(t, b.Seal())

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	results := NewResults(testOpts)
	_, _, err = results.Add(testDoc1())
	require.NoError(t, err)

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Close().Return(nil),
		exec.EXPECT().Close().Return(nil),
	)
	exhaustive, err := b.Query(Query{}, QueryOptions{Limit: 1}, results)
	require.NoError(t, err)
	require.False(t, exhaustive)

	rMap := results.Map()
	require.Equal(t, 1, rMap.Len())
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))
}

func TestBlockMockQueryMergeResultsDupeID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	exec := search.NewMockExecutor(ctrl)
	b.newExecutorFn = func() (search.Executor, error) {
		return exec, nil
	}

	results := NewResults(testOpts)
	_, _, err = results.Add(testDoc1())
	require.NoError(t, err)

	dIter := doc.NewMockIterator(ctrl)
	gomock.InOrder(
		exec.EXPECT().Execute(gomock.Any()).Return(dIter, nil),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(testDoc1DupeID()),
		dIter.EXPECT().Next().Return(true),
		dIter.EXPECT().Current().Return(testDoc2()),
		dIter.EXPECT().Next().Return(false),
		dIter.EXPECT().Err().Return(nil),
		dIter.EXPECT().Close().Return(nil),
		exec.EXPECT().Close().Return(nil),
	)
	exhaustive, err := b.Query(Query{}, QueryOptions{}, results)
	require.NoError(t, err)
	require.True(t, exhaustive)

	rMap := results.Map()
	require.Equal(t, 2, rMap.Len())
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))

	t2, ok := rMap.Get(ident.StringID(string(testDoc2().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		ident.NewTagsIterator(t2)))
}

func TestBlockBootstrapAddsSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	require.NoError(t, b.Bootstrap([]segment.Segment{seg1}))
	require.Equal(t, 1, len(b.bootstrappedSegments))
	require.Equal(t, seg1, b.bootstrappedSegments[0])
}

func TestBlockBootstrapAfterCloseFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Close())

	seg1 := segment.NewMockMutableSegment(ctrl)
	require.Error(t, blk.Bootstrap([]segment.Segment{seg1}))
}

func TestBlockBootstrapAfterSealWorks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Seal())

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	require.NoError(t, b.Bootstrap([]segment.Segment{seg1}))
	require.Equal(t, 1, len(b.bootstrappedSegments))
	require.Equal(t, seg1, b.bootstrappedSegments[0])
}

func TestBlockTickSingleSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	b.segment = seg1
	seg1.EXPECT().Size().Return(int64(10))

	result, err := blk.Tick(nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.NumSegments)
	require.Equal(t, int64(10), result.NumDocs)
}

func TestBlockTickMultipleSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	b.segment = seg1
	seg1.EXPECT().Size().Return(int64(10))

	seg2 := segment.NewMockMutableSegment(ctrl)
	seg2.EXPECT().Size().Return(int64(20))
	b.bootstrappedSegments = append(b.bootstrappedSegments, seg2)

	result, err := blk.Tick(nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.NumSegments)
	require.Equal(t, int64(30), result.NumDocs)
}

func TestBlockTickAfterSeal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Seal())

	b, ok := blk.(*block)
	require.True(t, ok)

	seg1 := segment.NewMockMutableSegment(ctrl)
	b.segment = seg1
	seg1.EXPECT().Size().Return(int64(10))

	result, err := blk.Tick(nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.NumSegments)
	require.Equal(t, int64(10), result.NumDocs)
}

func TestBlockTickAfterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	start := time.Now().Truncate(time.Hour)
	blk, err := NewBlock(start, time.Hour, testOpts)
	require.NoError(t, err)
	require.NoError(t, blk.Close())

	_, err = blk.Tick(nil)
	require.Error(t, err)
}

func TestBlockE2EInsertQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h1.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	h2 := NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h2.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
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
	results := NewResults(testOpts)
	exhaustive, err := b.Query(Query{q}, QueryOptions{}, results)
	require.NoError(t, err)
	require.True(t, exhaustive)
	require.Equal(t, 2, results.Size())

	rMap := results.Map()
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))

	t2, ok := rMap.Get(ident.StringID(string(testDoc2().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		ident.NewTagsIterator(t2)))
}

func TestBlockE2EInsertQueryLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h1.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	h2 := NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h2.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
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
	results := NewResults(testOpts)
	exhaustive, err := b.Query(Query{q}, QueryOptions{Limit: 1}, results)
	require.NoError(t, err)
	require.False(t, exhaustive)
	require.Equal(t, 1, results.Size())

	rMap := results.Map()
	numFound := 0
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	if ok {
		numFound++
		require.True(t, ident.NewTagIterMatcher(
			ident.MustNewTagStringsIterator("bar", "baz")).Matches(
			ident.NewTagsIterator(t1)))
	}

	t2, ok := rMap.Get(ident.StringID(string(testDoc2().ID)))
	if ok {
		numFound++
		require.True(t, ident.NewTagIterMatcher(
			ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
			ident.NewTagsIterator(t2)))
	}

	require.Equal(t, 1, numFound)
}

func TestBlockE2EInsertBootstrapQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h1.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	h2 := NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h2.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
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
	require.NoError(t, b.Bootstrap([]segment.Segment{seg}))

	q, err := idx.NewRegexpQuery([]byte("bar"), []byte("b.*"))
	require.NoError(t, err)
	results := NewResults(testOpts)
	exhaustive, err := b.Query(Query{q}, QueryOptions{}, results)
	require.NoError(t, err)
	require.True(t, exhaustive)
	require.Equal(t, 2, results.Size())

	rMap := results.Map()
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))

	t2, ok := rMap.Get(ident.StringID(string(testDoc2().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		ident.NewTagsIterator(t2)))
}

func TestBlockE2EInsertBootstrapMergeQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := time.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	blk, err := NewBlock(blockStart, blockSize, testOpts)
	require.NoError(t, err)
	b, ok := blk.(*block)
	require.True(t, ok)

	h1 := NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(xtime.ToUnixNano(blockStart))
	h1.EXPECT().OnIndexSuccess(xtime.ToUnixNano(blockStart))

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())

	res, err := b.WriteBatch(batch)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.NumSuccess)
	require.Equal(t, int64(0), res.NumError)

	seg := testSegment(t, testDoc2())
	require.NoError(t, b.Bootstrap([]segment.Segment{seg}))

	q, err := idx.NewRegexpQuery([]byte("bar"), []byte("b.*"))
	require.NoError(t, err)
	results := NewResults(testOpts)
	exhaustive, err := b.Query(Query{q}, QueryOptions{}, results)
	require.NoError(t, err)
	require.True(t, exhaustive)
	require.Equal(t, 2, results.Size())

	rMap := results.Map()
	t1, ok := rMap.Get(ident.StringID(string(testDoc1().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz")).Matches(
		ident.NewTagsIterator(t1)))

	t2, ok := rMap.Get(ident.StringID(string(testDoc2().ID)))
	require.True(t, ok)
	require.True(t, ident.NewTagIterMatcher(
		ident.MustNewTagStringsIterator("bar", "baz", "some", "more")).Matches(
		ident.NewTagsIterator(t2)))
}

func testSegment(t *testing.T, docs ...doc.Document) segment.Segment {
	seg, err := mem.NewSegment(0, testOpts.MemSegmentOptions())
	require.NoError(t, err)

	for _, d := range docs {
		_, err = seg.Insert(d)
		require.NoError(t, err)
	}

	return seg
}

func testDoc1() doc.Document {
	return doc.Document{
		ID: []byte("foo"),
		Fields: []doc.Field{
			doc.Field{
				Name:  []byte("bar"),
				Value: []byte("baz"),
			},
		},
	}
}

func testDoc1DupeID() doc.Document {
	return doc.Document{
		ID: []byte("foo"),
		Fields: []doc.Field{
			doc.Field{
				Name:  []byte("why"),
				Value: []byte("not"),
			},
			doc.Field{
				Name:  []byte("some"),
				Value: []byte("more"),
			},
		},
	}
}

func testDoc2() doc.Document {
	return doc.Document{
		ID: []byte("something"),
		Fields: []doc.Field{
			doc.Field{
				Name:  []byte("bar"),
				Value: []byte("baz"),
			},
			doc.Field{
				Name:  []byte("some"),
				Value: []byte("more"),
			},
		},
	}
}
