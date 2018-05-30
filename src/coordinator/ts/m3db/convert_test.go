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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConversion(t *testing.T) {
	iterator := BuildTestSeriesIterator(t)
	iterators := encoding.NewSeriesIterators([]encoding.SeriesIterator{iterator}, nil)
	iterAlloc := func(r io.Reader) encoding.ReaderIterator {
		iter := m3tsz.NewDecoder(true, encoding.NewOptions())
		return iter.Decode(r)
	}

	blocks, err := ConvertM3DBSeriesIterators(iterators, iterAlloc)
	require.NoError(t, err)

	for _, block := range blocks {
		assert.Equal(t, "foo", block.ID.String())
		assert.Equal(t, "namespace", block.Namespace.String())

		blockOne := block.Blocks[0].SeriesIterator
		blockTwo := block.Blocks[1].SeriesIterator

		start = time.Now().Truncate(time.Hour).Add(2 * time.Minute)
		middle = start.Add(30 * time.Minute)
		end = middle.Add(30 * time.Minute)

		assert.Equal(t, start, blockOne.Start())
		// assert.Equal(t, middle, blockOne.End())

		for i := 1; blockOne.Next(); i++ {
			dp, _, _ := blockOne.Current()
			assert.Equal(t, float64(i), dp.Value)
		}

		for i := 101; blockTwo.Next(); i++ {
			dp, _, _ := blockTwo.Current()
			assert.Equal(t, float64(i), dp.Value)
		}
	}
	fmt.Println(blocks)

}

// remove all of this once https://github.com/m3db/m3db/pull/705 lands

const (
	seriesID        = "foo"
	seriesNamespace = "namespace"
)

var (
	testTags = map[string]string{"foo": "bar", "baz": "qux"}

	blockSize = time.Hour / 2

	start  = time.Now().Truncate(time.Hour).Add(2 * time.Minute)
	middle = start.Add(blockSize)
	end    = middle.Add(blockSize)

	testIterAlloc = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
)

// Builds a MultiReaderIterator representing a single replica
// with two segments, one merged with values from 1->30, and
// one which is unmerged with 2 segments from 101->130
// with one of the unmerged containing even points, other containing odd
func buildReplica(t *testing.T) encoding.MultiReaderIterator {
	// Build a merged BlockReader
	encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	i := 0
	for at := time.Duration(0); at < blockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: start.Add(at), Value: float64(i)}
		err := encoder.Encode(datapoint, xtime.Second, nil)
		assert.NoError(t, err)
	}
	segment := encoder.Discard()
	blockStart := start.Truncate(blockSize)
	mergedReader := xio.BlockReader{
		SegmentReader: xio.NewSegmentReader(segment),
		Start:         blockStart,
		BlockSize:     blockSize,
	}

	// Build two unmerged BlockReaders
	i = 100
	encoder = m3tsz.NewEncoder(middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	encoderTwo := m3tsz.NewEncoder(middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	useFirstEncoder := true
	for at := time.Duration(0); at < blockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: middle.Add(at), Value: float64(i)}
		var err error
		if useFirstEncoder {
			err = encoder.Encode(datapoint, xtime.Second, nil)
		} else {
			err = encoderTwo.Encode(datapoint, xtime.Second, nil)
		}

		assert.NoError(t, err)
		useFirstEncoder = !useFirstEncoder
	}

	segment = encoder.Discard()
	segmentTwo := encoderTwo.Discard()
	secondBlockStart := blockStart.Add(blockSize)
	unmergedReaders := []xio.BlockReader{
		{
			SegmentReader: xio.NewSegmentReader(segment),
			Start:         secondBlockStart,
			BlockSize:     blockSize,
		},
		{
			SegmentReader: xio.NewSegmentReader(segmentTwo),
			Start:         secondBlockStart.Add(time.Minute),
			BlockSize:     blockSize,
		},
	}

	multiReader := encoding.NewMultiReaderIterator(testIterAlloc, nil)
	sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator([][]xio.BlockReader{
		{mergedReader},
		unmergedReaders,
	})

	multiReader.ResetSliceOfSlices(sliceOfSlicesIter)
	return multiReader
}

// BuildTestSeriesIterator creates a sample seriesIterator
// This series iterator has two identical replicas.
// Each replica has two blocks.
// The first block in each replica is merged and has values 1->30
// The second block is unmerged; when it was merged, it has values 101 -> 130
// from two readers, one with even values and other with odd values
// SeriesIterator ID is 'foo', namespace is 'namespace'
// Tags are "foo": "bar" and "baz": "qux"
func BuildTestSeriesIterator(t *testing.T) encoding.SeriesIterator {
	replicaOne := buildReplica(t)
	replicaTwo := buildReplica(t)

	tags := ident.Tags{}
	for name, value := range testTags {
		tags.Append(ident.StringTag(name, value))
	}

	return encoding.NewSeriesIterator(
		ident.StringID(seriesID),
		ident.StringID(seriesNamespace),
		ident.NewTagsIterator(tags),
		start,
		end,
		[]encoding.MultiReaderIterator{
			replicaOne,
			replicaTwo,
		},
		nil,
	)
}
