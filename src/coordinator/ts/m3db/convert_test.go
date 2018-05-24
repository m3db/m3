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

	"github.com/m3db/m3db/encoding"

	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/ts"

	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
)

var (
	testFetchTaggedTimeUnit = xtime.Millisecond
)

type testDatapoints []ts.Datapoint

func newTestDatapoints(num int, start, end time.Time) testDatapoints {
	dps := make(testDatapoints, 0, num)
	step := end.Sub(start) / time.Duration(num)
	for i := 0; i < num; i++ {
		dps = append(dps, ts.Datapoint{
			Timestamp: start.Add(step * time.Duration(i)),
			Value:     float64(i),
		})
	}
	return dps
}

func newSeriesIterators(ctrl *gomock.Controller, id ident.ID) encoding.SeriesIterator {
	// now := time.Now()
	// end := now.Add(-5 * time.Minute)
	// start := now.Add(-20 * time.Minute)

	// tagIter := ident.MustNewTagStringsIterator("app", "m3coordinator", "env", "production")

	// encoderPool := encoding.NewEncoderPool(nil)
	// encodingOpts := encoding.NewOptions().SetEncoderPool(encoderPool)
	// encoderPool.Init(func() encoding.Encoder {
	// 	return m3tsz.NewEncoder(time.Time{}, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	// })

	// td := newTestDatapoints(10, start, end)

	// enc := encoderPool.Get()
	// enc.Reset(start, len(td))
	// for _, dp := range td {
	// 	enc.Encode(dp, testFetchTaggedTimeUnit, nil)
	// }
	// reader := enc.Stream()
	// if reader == nil {
	// 	return nil
	// }
	// res, err := convert.ToSegments([]xio.BlockReader{
	// 	xio.BlockReader{
	// 		SegmentReader: reader,
	// 	},
	// })

	// // blockReader := encoding.

	// multiReaderIterator := encoding.NewMultiReaderIterator()

	// iter := encoding.NewSeriesIterator(id, "metrics", tagIter, start, end)

	blockSize := 2 * time.Hour

	now := time.Now()

	start := now.Truncate(time.Hour).Add(2 * time.Minute)
	end := start.Add(30 * time.Minute)
	multiReader1 := newReader(start, end, blockSize)

	start = end
	end = end.Add(30 * time.Minute)
	multiReader2 := newReader(start, end, blockSize)

	iterAlloc := func(r io.Reader) encoding.ReaderIterator {
		iter := m3tsz.NewDecoder(true, encoding.NewOptions())
		return iter.Decode(r)
	}

	blockStart := start.Truncate(blockSize)

	multiReader := encoding.NewMultiReaderIterator(iterAlloc, nil)
	multiReader.Reset([]xio.SegmentReader{multiReader1, multiReader2}, blockStart, blockSize)

	orig := encoding.NewSeriesIterator(ident.StringID("foo"), ident.StringID("namespace"),
		ident.NewTagSliceIterator(ident.Tags{}), start, end, []encoding.MultiReaderIterator{multiReader1, multiReader2}, nil)

	return orig
}

func newReader(start, end time.Time, blockSize time.Duration) xio.SegmentReader {
	encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil), true, encoding.NewOptions())

	i := 0
	for at := start; at.Before(end); at = at.Add(time.Minute) {
		datapoint := ts.Datapoint{Timestamp: at, Value: float64(i + 1)}
		if err := encoder.Encode(datapoint, xtime.Second, nil); err != nil {
			panic(err)
		}

		i++
	}

	segment := encoder.Discard()

	reader := xio.NewSegmentReader(segment)

	return reader
}

func ConversionTest(t *testing.T) {
	ctrl := gomock.NewController(t)
	// seriesIterator := newSeriesIterators(ctrl, ident.StringID("test"))
	// create seriesIterator
	//

	iterators := encoding.NewSeriesIterators([]encoding.SeriesIterator{seriesIterator, seriesIterator}, nil)

	blocks, _ := convertM3DBSeriesIterators(iterators)

}
