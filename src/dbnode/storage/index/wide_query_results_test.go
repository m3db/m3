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
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/stretchr/testify/assert"
)

func TestWideSeriesResults(t *testing.T) {
	var (
		ns        = ident.StringID("ns")
		batchSize = 20
		bytesPool = pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, nil)
		})
		idPool  = ident.NewPool(bytesPool, ident.PoolOptions{})
		batchCh = make(chan *ident.IDBatch)
		opts    = QueryResultsOptions{}

		elementCount = 48
		docBatchSize = 6
		doneCh       = make(chan struct{})
	)

	bytesPool.Init()
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

	go func() {
		i := 0
		for batch := range batchCh {
			batchStr := make([]string, 0, len(batch.IDs))
			for _, b := range batch.IDs {
				batchStr = append(batchStr, b.String())
			}

			batch.Done()
			fmt.Println(i, "Batch", batchStr)
			withinIndex := i < len(expected)
			assert.True(t, withinIndex)
			if withinIndex {
				fmt.Println(i, "Ex   ", expected[i])
				assert.Equal(t, expected[i], batchStr)
			}
			i++
		}
		doneCh <- struct{}{}
	}()

	docBatches := int(math.Ceil(float64(elementCount) / float64(docBatchSize)))
	docs := make([][]doc.Document, 0, docBatches)
	for i := 0; i < docBatches; i++ {
		batch := make([]doc.Document, 0, docBatchSize)
		for j := 0; j < docBatchSize; j++ {
			val := i*docBatchSize + j
			if val < elementCount {
				batch = append(batch, doc.Document{
					ID: []byte(fmt.Sprintf("foo%d", i*docBatchSize+j)),
				})
			}
		}

		docs = append(docs, batch)
	}

	wideRes := NewWideQueryResults(ns, batchSize, idPool, batchCh, opts)
	for _, docBatch := range docs {
		size, docsCount, err := wideRes.AddDocuments(docBatch)
		assert.Equal(t, 0, size)
		assert.Equal(t, 0, docsCount)
		assert.NoError(t, err)
	}

	wideRes.Finalize()
	<-doneCh
}
