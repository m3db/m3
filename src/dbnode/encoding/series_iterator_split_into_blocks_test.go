// Copyright (c) 2016 Uber Technologies, Inc.
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

//TODO(arnikola) convert to example
package encoding_test

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Series struct {
	ID     ident.ID
	Blocks []SeriesBlock
}

type SeriesBlock struct {
	Start          time.Time
	BlockSize      time.Duration
	Replicas       []encoding.MultiReaderIterator
	ValuesIterator encoding.SeriesIterator
}

func TestDeconstructAndReconstruct(t *testing.T) {
	blockSize := 2 * time.Hour

	now := time.Now()

	start := now.Truncate(time.Hour).Add(2 * time.Minute)
	end := start.Add(30 * time.Minute)

	encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil), true, encoding.NewOptions())

	i := 0
	for at := start; at.Before(end); at = at.Add(time.Minute) {
		datapoint := ts.Datapoint{Timestamp: at, Value: float64(i + 1)}
		err := encoder.Encode(datapoint, xtime.Second, nil)
		assert.NoError(t, err)
		i++
	}

	iterAlloc := func(r xio.Reader64, _ namespace.SchemaDescr) encoding.ReaderIterator {
		iter := m3tsz.NewDecoder(true, encoding.NewOptions())
		return iter.Decode(r)
	}

	segment := encoder.Discard()

	blockStart := start.Truncate(blockSize)

	reader := xio.NewSegmentReader(segment)

	multiReader := encoding.NewMultiReaderIterator(iterAlloc, nil)
	multiReader.Reset([]xio.SegmentReader{reader}, blockStart, blockSize, nil)

	orig := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
		ID:             ident.StringID("foo"),
		Namespace:      ident.StringID("namespace"),
		StartInclusive: xtime.ToUnixNano(start),
		EndExclusive:   xtime.ToUnixNano(end),
		Replicas:       []encoding.MultiReaderIterator{multiReader},
	}, nil)

	// Construct a per block view of the series
	series := Series{
		ID: orig.ID(),
	}

	replicas, err := orig.Replicas()
	require.NoError(t, err)
	// Collect all the replica per-block readers
	for _, replica := range replicas {
		perBlockSliceReaders := replica.Readers()
		next := true
		for next {
			// we are at a block
			l, start, bs := perBlockSliceReaders.CurrentReaders()

			var readers []xio.SegmentReader
			for i := 0; i < l; i++ {
				// reader to an unmerged (or already merged) block buffer
				reader := perBlockSliceReaders.CurrentReaderAt(i)

				// import to clone the reader as we need its position reset before
				// we use the contents of it again
				clonedReader, err := reader.Clone(nil)
				require.NoError(t, err)

				readers = append(readers, clonedReader)
			}

			iter := encoding.NewMultiReaderIterator(iterAlloc, nil)
			iter.Reset(readers, start, bs, nil)

			inserted := false
			for i := range series.Blocks {
				if series.Blocks[i].Start.Equal(start) {
					inserted = true
					series.Blocks[i].Replicas = append(series.Blocks[i].Replicas, iter)
					break
				}
			}
			if !inserted {
				series.Blocks = append(series.Blocks, SeriesBlock{
					Start:     start,
					BlockSize: bs,
					Replicas:  []encoding.MultiReaderIterator{iter},
				})
			}

			next = perBlockSliceReaders.Next()
		}
	}

	// Now per-block readers all collected, construct the per-block value
	// iterator combining all readers from the different replica readers
	for i, block := range series.Blocks {

		filterValuesStart := orig.Start()
		if block.Start.After(orig.Start()) {
			filterValuesStart = block.Start
		}

		end := block.Start.Add(block.BlockSize)

		filterValuesEnd := orig.End()
		if end.Before(filterValuesEnd) {
			filterValuesEnd = end
		}

		valuesIter := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
			ID:             orig.ID(),
			Namespace:      orig.Namespace(),
			Tags:           orig.Tags(),
			StartInclusive: xtime.ToUnixNano(filterValuesStart),
			EndExclusive:   xtime.ToUnixNano(filterValuesEnd),
			Replicas:       block.Replicas,
		}, nil)

		require.Len(t, block.Replicas, 1)
		series.Blocks[i].ValuesIterator = valuesIter
	}

	// Now show how we can iterate per block
	for _, block := range series.Blocks {
		iter := block.ValuesIterator
		blockCount := 0
		for iter.Next() {
			dp, _, _ := iter.Current()

			assert.Equal(t, dp.Timestamp, start.Add(time.Minute*time.Duration(blockCount)))
			assert.Equal(t, dp.Value, float64(blockCount+1))
			blockCount++
		}
		assert.NoError(t, iter.Err())
	}

	// Close once from the original series iterator to release all resources at once
	orig.Close()
}
