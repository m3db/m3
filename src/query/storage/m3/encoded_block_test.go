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

package m3

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildBlock(
	count int,
	now xtime.UnixNano,
	ctrl *gomock.Controller,
) *encodedBlock {
	iters := make([]encoding.SeriesIterator, count)
	seriesMetas := make([]block.SeriesMeta, count)
	for i := range iters {
		it := encoding.NewMockSeriesIterator(ctrl)
		it.EXPECT().Next().Return(true)
		dp := ts.Datapoint{
			TimestampNanos: now,
			Value:          float64(i),
		}

		it.EXPECT().Current().Return(dp, xtime.Second, nil)
		it.EXPECT().Next().Return(false)
		it.EXPECT().Err().Return(nil)
		it.EXPECT().Close()
		iters[i] = it
		seriesMetas[i] = block.SeriesMeta{Name: []byte(fmt.Sprint(i))}
	}

	return &encodedBlock{
		seriesBlockIterators: iters,
		seriesMetas:          seriesMetas,
		options:              NewOptions(encoding.NewOptions()),
	}
}

func TestMultiSeriesIter(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := xtime.Now()
	tests := []struct {
		concurrency int
		sizes       []int
	}{
		{1, []int{12}},
		{2, []int{6, 6}},
		{3, []int{4, 4, 4}},
		{4, []int{3, 3, 3, 3}},
		{5, []int{3, 3, 2, 2, 2}},
		{6, []int{2, 2, 2, 2, 2, 2}},
		{7, []int{2, 2, 2, 2, 2, 1, 1}},
		{8, []int{2, 2, 2, 2, 1, 1, 1, 1}},
		{9, []int{2, 2, 2, 1, 1, 1, 1, 1, 1}},
		{10, []int{2, 2, 1, 1, 1, 1, 1, 1, 1, 1}},
		{11, []int{2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{12, []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
		{13, []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0}},
	}

	for _, tt := range tests {
		b := buildBlock(12, now, ctrl)
		batch, err := b.MultiSeriesIter(tt.concurrency)
		require.NoError(t, err)

		count := 0
		iterCount := 0.0
		for i, b := range batch {
			require.Equal(t, tt.sizes[i], b.Size)
			iter := b.Iter
			require.Equal(t, tt.sizes[i], iter.SeriesCount())

			// Ensure that all metas are split as expected.
			metas := iter.SeriesMeta()
			for _, m := range metas {
				assert.Equal(t, string(m.Name), fmt.Sprint(count))
				count++
			}

			// Ensure that all iterators are split as expected.
			for iter.Next() {
				vals := iter.Current().Datapoints().Values()
				require.Equal(t, 1, len(vals))
				assert.Equal(t, iterCount, vals[0])
				iterCount++
			}

			assert.NoError(t, iter.Err())
		}

		assert.NoError(t, b.Close())
	}
}

func TestMultiSeriesIterError(t *testing.T) {
	b := &encodedBlock{options: NewOptions(encoding.NewOptions())}
	_, err := b.MultiSeriesIter(0)
	require.Error(t, err)

	_, err = b.MultiSeriesIter(-1)
	require.Error(t, err)
}
