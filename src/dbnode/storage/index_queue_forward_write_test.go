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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	m3ninxidx "github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestForwardWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Second)()

	ctx := context.NewContext()

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

	idxOpts := testNamespaceIndexOptions().
		SetInsertMode(index.InsertSync).
		SetForwardIndexProbability(1).
		SetForwardIndexThreshold(1)

	opts := testDatabaseOptions().
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
	idx, err := newNamespaceIndexWithInsertQueueFn(md, newFn, opts)
	assert.NoError(t, err)

	var (
		ts   = idx.(*nsIndex).state.latestBlock.StartTime()
		next = ts.Truncate(blockSize).Add(blockSize)
		id   = ident.StringID("foo")
		tags = ident.NewTags(
			ident.StringTag("name", "value"),
		)
		lifecycleFns = index.NewMockOnIndexSeries(ctrl)
	)

	lifecycleFns.EXPECT().OnIndexFinalize(xtime.ToUnixNano(ts))
	lifecycleFns.EXPECT().OnIndexSuccess(xtime.ToUnixNano(ts))

	lifecycleFns.EXPECT().OnIndexFinalize(xtime.ToUnixNano(ts.Add(blockSize)))
	lifecycleFns.EXPECT().OnIndexSuccess(xtime.ToUnixNano(ts.Add(blockSize)))

	lifecycleFns.EXPECT().NeedsIndexUpdate(xtime.ToUnixNano(next)).Return(true)
	lifecycleFns.EXPECT().OnIndexPrepare()

	entry, doc := testWriteBatchEntry(id, tags, now, lifecycleFns)
	batch := testWriteBatch(entry, doc, testWriteBatchBlockSizeOption(blockSize))
	assert.NoError(t, idx.WriteBatch(batch))

	defer idx.Close()

	reQuery, err := m3ninxidx.NewRegexpQuery([]byte("name"), []byte("val.*"))
	assert.NoError(t, err)

	// NB: query both the current and the next index block to ensure that the
	// write was correctly indexed to both.
	for _, start := range []time.Time{now, now.Add(blockSize)} {
		res, err := idx.Query(ctx, index.Query{Query: reQuery}, index.QueryOptions{
			StartInclusive: start.Add(-1 * time.Minute),
			EndExclusive:   start.Add(1 * time.Minute),
		})
		require.NoError(t, err)

		assert.True(t, res.Exhaustive)
		results := res.Results
		assert.Equal(t, "testns1", results.Namespace().String())

		tags, ok := results.Map().Get(ident.StringID("foo"))
		assert.True(t, ok)
		assert.True(t, ident.NewTagIterMatcher(
			ident.MustNewTagStringsIterator("name", "value")).Matches(
			ident.NewTagsIterator(tags)))
	}
}
