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

package test

import (
	"io"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"

	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

var (
	// SeriesID is the expected id for the generated series
	SeriesID string
	// SeriesNamespace is the expected namespace for the generated series
	SeriesNamespace string
	// TestTags is the expected tags for the generated series
	TestTags map[string]string
	// BlockSize is the expected block size for the generated series
	BlockSize time.Duration
	// Start is the expected start time for the first block in the generated series
	Start time.Time
	// SeriesStart is the expected start time for the generated series
	SeriesStart time.Time
	// Middle is the expected end for the first block, and start of the second block
	Middle time.Time
	// End is the expected end time for the generated series
	End time.Time

	testIterAlloc func(r io.Reader) encoding.ReaderIterator
)

func init() {
	SeriesID = "id"
	SeriesNamespace = "namespace"

	TestTags = map[string]string{"foo": "bar", "baz": "qux"}

	BlockSize = time.Hour / 2

	Start = time.Now().Truncate(time.Hour)
	SeriesStart = Start.Add(2 * time.Minute)
	Middle = Start.Add(BlockSize)
	End = Middle.Add(BlockSize)

	testIterAlloc = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
}

// Builds a MultiReaderIterator representing a single replica
// with two segments, one merged with values from 1->30, and
// one which is unmerged with 2 segments from 101->130
// with one of the unmerged containing even points, other containing odd
func buildReplica() (encoding.MultiReaderIterator, error) {
	// Build a merged BlockReader
	encoder := m3tsz.NewEncoder(Start, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	i := 0
	for at := time.Duration(0); at < BlockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: Start.Add(at), Value: float64(i)}
		err := encoder.Encode(datapoint, xtime.Second, nil)
		if err != nil {
			return nil, err
		}
	}
	segment := encoder.Discard()
	mergedReader := xio.BlockReader{
		SegmentReader: xio.NewSegmentReader(segment),
		Start:         Start,
		BlockSize:     BlockSize,
	}

	// Build two unmerged BlockReaders
	i = 100
	encoder = m3tsz.NewEncoder(Middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	encoderTwo := m3tsz.NewEncoder(Middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	useFirstEncoder := true

	for at := time.Duration(0); at < BlockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: Middle.Add(at), Value: float64(i)}
		var err error
		if useFirstEncoder {
			err = encoder.Encode(datapoint, xtime.Second, nil)
		} else {
			err = encoderTwo.Encode(datapoint, xtime.Second, nil)
		}
		if err != nil {
			return nil, err
		}
		useFirstEncoder = !useFirstEncoder
	}

	segment = encoder.Discard()
	segmentTwo := encoderTwo.Discard()
	unmergedReaders := []xio.BlockReader{
		{
			SegmentReader: xio.NewSegmentReader(segment),
			Start:         Middle,
			BlockSize:     BlockSize,
		},
		{
			SegmentReader: xio.NewSegmentReader(segmentTwo),
			Start:         Middle,
			BlockSize:     BlockSize,
		},
	}

	multiReader := encoding.NewMultiReaderIterator(testIterAlloc, nil)
	sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator([][]xio.BlockReader{
		{mergedReader},
		unmergedReaders,
	})

	multiReader.ResetSliceOfSlices(sliceOfSlicesIter)
	return multiReader, nil
}

// BuildTestSeriesIterator creates a sample SeriesIterator
// This series iterator has two identical replicas.
// Each replica has two blocks.
// The first block in each replica is merged and has values 1->30
// The values 1 and 2 appear before the SeriesIterator start time, and are not expected
// to appear when reading through the iterator
// The second block is unmerged; when it was merged, it has values 101 -> 130
// from two readers, one with even values and other with odd values
// Expected data points for reading through the iterator: [3..30,101..130], 58 in total
// SeriesIterator ID is 'foo', namespace is 'namespace'
// Tags are "foo": "bar" and "baz": "qux"
func BuildTestSeriesIterator() (encoding.SeriesIterator, error) {
	replicaOne, err := buildReplica()
	if err != nil {
		return nil, err
	}
	replicaTwo, err := buildReplica()
	if err != nil {
		return nil, err
	}

	tags := ident.Tags{}
	for name, value := range TestTags {
		tags.Append(ident.StringTag(name, value))
	}

	return encoding.NewSeriesIterator(
		ident.StringID(SeriesID),
		ident.StringID(SeriesNamespace),
		ident.NewTagsIterator(tags),
		SeriesStart,
		End,
		[]encoding.MultiReaderIterator{
			replicaOne,
			replicaTwo,
		},
		nil,
	), nil
}
