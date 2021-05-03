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
	"github.com/m3db/m3/src/x/instrument"
)

func TestFetchResultIterTest(t *testing.T) {
	mocks := gomock.NewController(t)
	defer mocks.Finish()

	ctx, nsID, resMap, start, end, db := setup(mocks)
	blockPermits := &fakePermits{available: 5, quotaPerPermit: 5}
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
	// 20 permits are not acquired because the accounting is not 100% accurate. permits are not acquired until
	// after the block is processed, so a block might be eagerly processed and then permit acquisition fails.
	require.Equal(t, 19, blockPermits.acquired)
	require.Equal(t, 19, blockPermits.released)
}

func TestFetchResultIterTestNoReleaseWithoutAcquire(t *testing.T) {
	blockPermits := &fakePermits{available: 10, quotaPerPermit: 1000}
	emptyMap := index.NewQueryResults(ident.StringID("testNs"), index.QueryResultsOptions{}, testIndexOptions)
	iter := newFetchTaggedResultsIter(fetchTaggedResultsIterOpts{
		queryResult: index.QueryResult{
			Results: emptyMap,
		},
		blockPermits:    blockPermits,
		instrumentClose: func(err error) {},
	})
	ctx := context.NewBackground()
	for iter.Next(ctx) {
	}
	require.NoError(t, iter.Err())
	iter.Close(nil)

	require.Equal(t, 0, blockPermits.acquired)
	require.Equal(t, 0, blockPermits.released)
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
	context.Context, ident.ID, index.QueryResults, time.Time, time.Time, *storage.Mockdatabase,
) {
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
	return ctx, nsID, resMap, start, end, db
}

type fakePermits struct {
	acquired       int
	released       int
	available      int
	quotaPerPermit int64
}

func (p *fakePermits) Acquire(_ context.Context) (permits.Permit, error) {
	if p.available == 0 {
		return nil, errors.New("available should never be 0")
	}
	p.available--
	p.acquired++
	return permits.NewPermit(p.quotaPerPermit, instrument.NewOptions()), nil
}

func (p *fakePermits) TryAcquire(_ context.Context) (permits.Permit, error) {
	if p.available == 0 {
		return nil, nil
	}
	p.available--
	p.acquired++
	return permits.NewPermit(p.quotaPerPermit, instrument.NewOptions()), nil
}

func (p *fakePermits) Release(_ permits.Permit) {
	p.released++
	p.available++
}
