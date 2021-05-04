// Copyright (c) 2021  Uber Technologies, Inc.
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

package read

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts"
)

func TestNewM3QueryResultIterator(t *testing.T) {
	numSeries := 3
	series := makeSeries(numSeries)
	result := Result{
		Series:    series,
		Meta:      block.NewResultMetadata(),
		BlockType: 0,
	}

	iter := NewM3QueryResultIterator(result)

	for i := 0; i < numSeries; i++ {
		require.True(t, iter.Next())
		require.Equal(t, series[i], iter.Current())
	}

	require.False(t, iter.Next())
	require.Nil(t, iter.Current())
}

func makeSeries(numSeries int) []*ts.Series {
	start := time.Unix(1535948880, 0)
	series := make([]*ts.Series, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		series = append(series, ts.NewSeries([]byte("bar"),
			ts.NewFixedStepValues(10*time.Second, 2, 2, start),
			test.TagSliceToTags([]models.Tag{
				{Name: []byte("baz"), Value: []byte("bar")},
				{Name: []byte("qaz"), Value: []byte("qux")},
			})))
	}

	return series
}
