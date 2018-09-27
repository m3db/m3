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

package m3db

import (
	"io"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testTags = test.TestTags

	start  = test.Start
	middle = test.Middle
	end    = test.End

	testIterAlloc = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
)

func TestConversion(t *testing.T) {
	iter, err := test.BuildTestSeriesIterator()
	pools := test.MakeMockIteratorPool()
	require.NoError(t, err)
	iterators := encoding.NewSeriesIterators([]encoding.SeriesIterator{iter}, nil)

	blocks, err := convertM3DBSeriesIterators(iterators, testIterAlloc, pools)
	require.NoError(t, err)

	for _, block := range blocks {
		blockOneSeriesIterator := block[0]
		blockTwoSeriesIterator := block[1]

		assert.Equal(t, start.Add(2*time.Minute), blockOneSeriesIterator.Start())
		assert.Equal(t, middle, blockOneSeriesIterator.End())
		checkTags(t, blockOneSeriesIterator.Tags())

		for i := 3; blockOneSeriesIterator.Next(); i++ {
			dp, _, _ := blockOneSeriesIterator.Current()
			assert.Equal(t, float64(i), dp.Value)
			assert.Equal(t, start.Add(time.Duration(i-1)*time.Minute), dp.Timestamp)
		}

		assert.Equal(t, middle, blockTwoSeriesIterator.Start())
		assert.Equal(t, end, blockTwoSeriesIterator.End())
		checkTags(t, blockTwoSeriesIterator.Tags())

		for i, j := 101, 1; blockTwoSeriesIterator.Next(); i++ {
			dp, _, _ := blockTwoSeriesIterator.Current()
			assert.Equal(t, float64(i), dp.Value)
			assert.Equal(t, middle.Add(time.Duration(j-1)*time.Minute), dp.Timestamp)
			j++
		}
	}
}

func checkTags(t *testing.T, tags ident.TagIterator) {
	convertedTags, err := storage.FromIdentTagIteratorToTags(tags)
	require.NoError(t, err)
	assert.Equal(t, models.Tags{{"baz", testTags["baz"]}, {"foo", testTags["foo"]}}, convertedTags)
}
