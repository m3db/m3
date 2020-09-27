// Copyright (c) 2020 Uber Technologies, Inc.
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
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testNs     = ident.StringID("ns")
	testIDPool ident.Pool
)

func init() {
	// NB: build test pools once.
	bytesPool := pool.NewCheckedBytesPool(nil, nil,
		func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, nil)
		})

	testIDPool = ident.NewPool(bytesPool, ident.PoolOptions{})
	bytesPool.Init()
}

func buildExpected(
	t *testing.T,
	elementCount int,
	batchSize int,
) [][]string {
	exBatches := int(math.Ceil(float64(elementCount) / float64(batchSize)))
	expected := make([][]string, 0, exBatches)
	for i := 0; i < exBatches; i++ {
		batch := make([]string, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			val := i*batchSize + j
			if val < elementCount {
				batch = append(batch, fmt.Sprintf("foo%d", val))
			}
		}

		expected = append(expected, batch)
	}

	return expected
}

func assertExpected(
	t *testing.T,
	expected [][]string,
	batchCh <-chan *ident.IDBatch,
	doneCh chan<- struct{},
) {
	go func() {
		i := 0
		for batch := range batchCh {
			batchStr := make([]string, 0, len(batch.IDs))
			for _, b := range batch.IDs {
				batchStr = append(batchStr, b.String())
				b.Finalize()
			}

			batch.Done()
			withinIndex := i < len(expected)
			assert.True(t, withinIndex)
			if withinIndex {
				assert.Equal(t, expected[i], batchStr)
			}
			i++
		}
		assert.Equal(t, len(expected), i)
		doneCh <- struct{}{}
	}()
}

func buildDocs(elementCount int, batchSize int) [][]doc.Document {
	docBatches := int(math.Ceil(float64(elementCount) / float64(batchSize)))
	docs := make([][]doc.Document, 0, docBatches)
	for i := 0; i < docBatches; i++ {
		batch := make([]doc.Document, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			val := i*batchSize + j
			if val < elementCount {
				val := fmt.Sprintf("foo%d", i*batchSize+j)
				batch = append(batch, doc.Document{
					ID: []byte(val),
				})
			}
		}

		docs = append(docs, batch)
	}

	return docs
}

func TestWideSeriesResults(t *testing.T) {
	// Test many different permutations of element count and batch sizes.
	max := 31
	// elementCount := 30
	// docBatchSize := 20
	// batchSize := 10
	for elementCount := 0; elementCount < max; elementCount++ {
		for docBatchSize := 1; docBatchSize < max; docBatchSize++ {
			for batchSize := 1; batchSize < max; batchSize++ {
				var (
					batchCh = make(chan *ident.IDBatch)
					doneCh  = make(chan struct{})
				)

				docs := buildDocs(elementCount, docBatchSize)
				expected := buildExpected(t, elementCount, batchSize)
				assertExpected(t, expected, batchCh, doneCh)

				wideQueryOptions := NewWideQueryOptions(
					time.Now(), batchSize, batchCh, time.Hour*2, nil, IterationOptions{})

				wideRes := NewWideQueryResults(testNs, testIDPool, nil, wideQueryOptions)
				for _, docBatch := range docs {
					size, docsCount, err := wideRes.AddDocuments(docBatch)
					assert.Equal(t, 0, size)
					assert.Equal(t, 0, docsCount)
					assert.NoError(t, err)
				}

				wideRes.Finalize()
				assert.Equal(t, 0, wideRes.TotalDocsCount())
				assert.Equal(t, 0, wideRes.Size())
				assert.Equal(t, "ns", wideRes.Namespace().String())
				<-doneCh
			}
		}
	}
}

func TestWideSeriesResultsWithShardFilter(t *testing.T) {
	var (
		elementCount = 100
		docBatchSize = 10
		batchSize    = 3

		batchCh = make(chan *ident.IDBatch)
		doneCh  = make(chan struct{})
	)

	docs := buildDocs(elementCount, docBatchSize)
	// NB: feed these in out of order to ensure WideQueryOptions sorts them.
	shards := []uint32{41, 1, 21, 33, 52}
	expected := [][]string{{"foo1", "foo21", "foo33"}, {"foo41"}}
	assertExpected(t, expected, batchCh, doneCh)

	wideQueryOptions := NewWideQueryOptions(
		time.Now(), batchSize, batchCh, time.Hour*2, shards, IterationOptions{})

	filter := func(id ident.ID) (uint32, bool) {
		i, err := strconv.Atoi(strings.TrimPrefix(id.String(), "foo"))
		require.NoError(t, err)
		// mark shard 52 as not owned by this node; should not appear in result
		return uint32(i), i != 52
	}

	wideRes := NewWideQueryResults(testNs, testIDPool, filter, wideQueryOptions)
	for i, docBatch := range docs {
		size, docsCount, err := wideRes.AddDocuments(docBatch)
		assert.Equal(t, 0, size)
		assert.Equal(t, 0, docsCount)
		if i < 52/docBatchSize {
			assert.NoError(t, err)
		} else {
			// NB: after doc `foo52` is added, wide result accumulation is exhausted.
			assert.Equal(t, ErrWideQueryResultsExhausted, err)
		}
	}

	wideRes.Finalize()
	assert.Equal(t, 0, wideRes.TotalDocsCount())
	assert.Equal(t, 0, wideRes.Size())
	assert.Equal(t, "ns", wideRes.Namespace().String())
	<-doneCh
}
