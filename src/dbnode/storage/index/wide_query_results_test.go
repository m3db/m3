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

func buildDocs(documentCount int, batchSize int) [][]doc.Document {
	docBatches := int(math.Ceil(float64(documentCount) / float64(batchSize)))
	docs := make([][]doc.Document, 0, docBatches)
	for i := 0; i < docBatches; i++ {
		batch := make([]doc.Document, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			val := i*batchSize + j
			if val < documentCount {
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

func buildExpected(t *testing.T, docs [][]doc.Document) [][]string {
	expected := make([][]string, 0, len(docs))
	for _, batch := range docs {
		idBatch := make([]string, 0, len(batch))
		for _, doc := range batch {
			idBatch = append(idBatch, string(doc.ID))
		}

		expected = append(expected, idBatch)
	}

	return expected
}

// drainAndCheckBatches kicks off a routine to drain incoming batches,
// comparing them against expected values, and notifying doneCh when all
// batches have been drained.
func drainAndCheckBatches(
	t *testing.T,
	expected [][]string,
	checkShard bool,
	batchCh <-chan *ident.IDBatch,
	doneCh chan<- struct{},
) {
	go func() {
		i := 0
		var exShard uint32
		for batch := range batchCh {
			batchStr := make([]string, 0, len(batch.ShardIDs))
			for _, shardID := range batch.ShardIDs {
				batchStr = append(batchStr, shardID.ID.String())
				shardID.ID.Finalize()
				if checkShard {
					assert.Equal(t, exShard, shardID.Shard)
				}
				exShard++
			}

			batch.Processed()
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

func testFilterFn(t *testing.T, id ident.ID) (uint32, bool) {
	i, err := strconv.Atoi(strings.TrimPrefix(id.String(), "foo"))
	require.NoError(t, err)
	// mark this shard as not owned by this node; should not appear in result
	return uint32(i), true
}

func TestWideSeriesResults(t *testing.T) {
	var (
		max       = 31
		blockSize = time.Hour * 2
		now       = time.Now().Truncate(blockSize)
	)

	// Test many different permutations of element count and batch sizes.
	for documentCount := 0; documentCount < max; documentCount++ {
		for docBatchSize := 1; docBatchSize < max; docBatchSize++ {
			for batchSize := 1; batchSize < max; batchSize++ {
				var (
					batchCh = make(chan *ident.IDBatch)
					doneCh  = make(chan struct{})
				)

				docs := buildDocs(documentCount, docBatchSize)
				// NB: expected should have docs split by `batchSize`
				expected := buildExpected(t, buildDocs(documentCount, batchSize))
				drainAndCheckBatches(t, expected, true, batchCh, doneCh)

				wideQueryOptions, err := NewWideQueryOptions(
					now, batchSize, blockSize, nil, IterationOptions{})

				require.NoError(t, err)

				filter := func(id ident.ID) (uint32, bool) {
					return testFilterFn(t, id)
				}

				wideRes := NewWideQueryResults(testNs, testIDPool, filter, batchCh, wideQueryOptions)
				var runningSize, batchDocCount int
				for _, docBatch := range docs {
					size, docsCount, err := wideRes.AddDocuments(docBatch)

					runningSize = (runningSize + len(docBatch)) % batchSize
					batchDocCount += len(docBatch)

					assert.Equal(t, runningSize, size)
					assert.Equal(t, batchDocCount, docsCount)
					assert.NoError(t, err)
				}

				wideRes.Finalize()
				assert.Equal(t, 0, wideRes.Size())
				assert.Equal(t, batchDocCount, wideRes.TotalDocsCount())
				assert.Equal(t, "ns", wideRes.Namespace().String())
				<-doneCh
			}
		}
	}
}

func TestWideSeriesResultsWithShardFilter(t *testing.T) {
	var (
		documentCount = 100
		docBatchSize  = 10
		batchSize     = 3

		// This shard is part of the shard set being queried, but it will be
		// flagged as not being owned by this node in the filter function, thus
		// it should not appear in the result.
		notOwnedShard = uint32(7)

		// After reading the last queried shard, add documents should
		// short-circuit future shards by returning shard exhausted error.
		lastQueriedShard           = 42
		batchesReadBeforeExhausted = lastQueriedShard / docBatchSize

		batchCh = make(chan *ident.IDBatch)
		doneCh  = make(chan struct{})

		blockSize = time.Hour * 2
		now       = time.Now().Truncate(blockSize)
	)

	docs := buildDocs(documentCount, docBatchSize)
	// NB: feed these in out of order to ensure WideQueryOptions sorts them.
	shards := []uint32{21, notOwnedShard, 41, uint32(lastQueriedShard), 1}
	expected := [][]string{{"foo1", "foo21", "foo41"}, {"foo42"}}
	drainAndCheckBatches(t, expected, false, batchCh, doneCh)

	wideQueryOptions, err := NewWideQueryOptions(
		now, batchSize, blockSize, shards, IterationOptions{})
	require.NoError(t, err)
	filter := func(id ident.ID) (uint32, bool) {
		i, _ := testFilterFn(t, id)
		// mark this shard as not owned by this node; should not appear in result
		owned := i != notOwnedShard
		return uint32(i), owned
	}

	wideRes := NewWideQueryResults(testNs, testIDPool, filter, batchCh, wideQueryOptions)
	assert.False(t, wideRes.EnforceLimits())

	var runningSize, batchDocCount int
	for i, docBatch := range docs {
		// NB: work out how many values this batch is expected to add.
		minInclusive := i * docBatchSize
		maxExclusive := minInclusive + docBatchSize
		for _, shard := range shards {
			if shard == notOwnedShard {
				continue
			}

			if minInclusive <= int(shard) && int(shard) < maxExclusive {
				batchDocCount++
				runningSize = (runningSize + 1) % batchSize
			}
		}

		size, docsCount, err := wideRes.AddDocuments(docBatch)
		if i < batchesReadBeforeExhausted {
			// NB: doc count and size should only increment while the results have
			// not been exhausted.
			assert.NoError(t, err)
		} else {
			// NB: after an element with the last shard is added,
			// wide result accumulation is exhausted.
			assert.Equal(t, ErrWideQueryResultsExhausted, err)
		}

		require.Equal(t, runningSize, size)
		assert.Equal(t, batchDocCount, docsCount)
	}

	wideRes.Finalize()
	assert.Equal(t, batchDocCount, wideRes.TotalDocsCount())
	assert.Equal(t, 0, wideRes.Size())
	assert.Equal(t, "ns", wideRes.Namespace().String())
	<-doneCh
}
