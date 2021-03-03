// Copyright (c) 2021 Uber Technologies, Inc.
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

package node

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/limits/permits"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
)

func TestFetchResultIterTest(t *testing.T) {
	mocks := gomock.NewController(t)
	defer mocks.Finish()

	scope, ctx, nsID, resMap, start, end, db := setup(mocks)

	blockPermits := &fakePermits{available: 5}
	iter := newFetchTaggedResultsIter(fetchTaggedResultsIterOpts{
		queryResult: index.QueryResult{
			Results: resMap,
		},
		queryOpts: index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
		},
		fetchData:       true,
		db:              db,
		nsID:            nsID,
		blockPermits:    blockPermits,
		blocksPerBatch:  5,
		nowFn:           time.Now,
		dataReadMetrics: index.NewQueryMetrics("", scope),
		totalMetrics:    index.NewQueryMetrics("", scope),
		seriesBlocks:    scope.Histogram("series-blocks", tally.MustMakeExponentialValueBuckets(10, 2, 5)),
		instrumentClose: func(err error) {},
	})
	total := 0
	for iter.Next(ctx) {
		total++
		require.NotNil(t, iter.Current())
		require.Len(t, iter.Current().(*idResult).blockReaders, 10)
	}
	require.NoError(t, iter.Err())
	iter.Close(nil)

	require.Equal(t, 10, total)
	require.Equal(t, 5, blockPermits.acquired)
	require.Equal(t, 5, blockPermits.released)
	requireSeriesBlockMetric(t, scope)
}

func TestFetchResultIterTestUnsetBlocksPerBatch(t *testing.T) {
	mocks := gomock.NewController(t)
	defer mocks.Finish()

	scope, ctx, nsID, resMap, start, end, db := setup(mocks)

	blockPermits := &fakePermits{available: 10}
	iter := newFetchTaggedResultsIter(fetchTaggedResultsIterOpts{
		queryResult: index.QueryResult{
			Results: resMap,
		},
		queryOpts: index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
		},
		fetchData:       true,
		db:              db,
		nsID:            nsID,
		blockPermits:    blockPermits,
		nowFn:           time.Now,
		dataReadMetrics: index.NewQueryMetrics("", scope),
		totalMetrics:    index.NewQueryMetrics("", scope),
		seriesBlocks:    scope.Histogram("series-blocks", tally.MustMakeExponentialValueBuckets(10, 2, 5)),
		instrumentClose: func(err error) {},
	})
	total := 0
	for iter.Next(ctx) {
		total++
		require.NotNil(t, iter.Current())
		require.Len(t, iter.Current().(*idResult).blockReaders, 10)
	}
	require.NoError(t, iter.Err())
	iter.Close(nil)

	require.Equal(t, 10, total)
	require.Equal(t, 10, blockPermits.acquired)
	require.Equal(t, 10, blockPermits.released)
	requireSeriesBlockMetric(t, scope)
}

func TestFetchResultIterTestForceBlocksPerBatch(t *testing.T) {
	blockPermits := &permits.LookbackLimitPermit{}
	resMap := index.NewQueryResults(ident.StringID("testNs"), index.QueryResultsOptions{}, testIndexOptions)
	iter := newFetchTaggedResultsIter(fetchTaggedResultsIterOpts{
		queryResult: index.QueryResult{
			Results: resMap,
		},
		blockPermits:   blockPermits,
		blocksPerBatch: 1000,
		nowFn:          time.Now,
	})
	downcast, ok := iter.(*fetchTaggedResultsIter)
	require.True(t, ok)
	require.Equal(t, 1, downcast.blocksPerBatch)
}

func requireSeriesBlockMetric(t *testing.T, scope tally.TestScope) {
	values, ok := scope.Snapshot().Histograms()["series-blocks+"]
	require.True(t, ok)

	sum := 0
	for _, count := range values.Values() {
		sum += int(count)
	}
	require.Equal(t, 1, sum)
}

func setup(mocks *gomock.Controller) (
	tally.TestScope, context.Context, ident.ID, index.QueryResults, time.Time, time.Time, *storage.Mockdatabase,
) {
	scope := tally.NewTestScope("", map[string]string{})
	ctx := context.NewBackground()
	nsID := ident.StringID("testNs")

	resMap := index.NewQueryResults(nsID,
		index.QueryResultsOptions{}, testIndexOptions)
	start := time.Now()
	end := start.Add(24 * time.Hour)
	db := storage.NewMockdatabase(mocks)

	// 10 series IDs
	for i := 0; i < 10; i++ {
		id := ident.StringID(fmt.Sprintf("seriesId_%d", i))
		var blockReaders [][]xio.BlockReader
		// 10 block readers per series
		for j := 0; j < 10; j++ {
			blockReaders = append(blockReaders, []xio.BlockReader{})
		}
		db.EXPECT().ReadEncoded(ctx, nsID, id, start, end).Return(&series.FakeBlockReaderIter{
			Readers: blockReaders,
		}, nil)
		resMap.Map().Set(id.Bytes(), doc.Document{})
	}
	return scope, ctx, nsID, resMap, start, end, db
}

type fakePermits struct {
	acquired  int
	released  int
	available int
}

func (p *fakePermits) Acquire(_ context.Context) error {
	if p.available == 0 {
		return errors.New("available should never be 0")
	}
	p.available--
	p.acquired++
	return nil
}

func (p *fakePermits) TryAcquire(_ context.Context) (bool, error) {
	if p.available == 0 {
		return false, nil
	}
	p.available--
	p.acquired++
	return true, nil
}

func (p *fakePermits) Release(_ int64) {
	p.released++
	p.available++
}
