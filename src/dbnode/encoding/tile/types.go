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

package tile

import (
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// SeriesFrameUnits describes units in this series frame.
type SeriesFrameUnits interface {
	// Value returns the annotation at the given frame index (or an error if index is out of bounds).
	Value(idx int) (xtime.Unit, error)
}

// SeriesFrameAnnotations describes annotations in this series frame.
type SeriesFrameAnnotations interface {
	// Value returns the annotation at the given frame index (or an error if index is out of bounds).
	Value(idx int) (ts.Annotation, error)
}

// SeriesFrameIterator is a frame-wise iterator across a series block.
type SeriesFrameIterator interface {
	// Err returns any errors encountered.
	Err() error
	// Next moves to the next element.
	Next() bool
	// Close closes the iterator.
	Close() error
	// Current returns the current series block frame.
	Current() SeriesBlockFrame
	// Reset resets the series frame iterator.
	Reset(
		start xtime.UnixNano,
		step time.Duration,
		iter fs.CrossBlockIterator,
	) error
}

// SeriesBlockIterator provides concurrent iteration across multiple series
// in a frame-wise fashion.
type SeriesBlockIterator interface {
	// Err returns any errors encountered.
	Err() error
	// Next moves to the next element.
	Next() bool
	// Close closes the iterator.
	Close() error
	// Current returns the next set of series frame iterators.
	Current() (SeriesFrameIterator, ident.BytesID, ts.EncodedTags)
}

// Options are series block iterator options.
type Options struct {
	// FrameSize is the frame size in nanos.
	FrameSize time.Duration
	// Start is the start time for the iterator in nanos from epoch.
	Start xtime.UnixNano
	// EncodingOpts are options for the encoder.
	EncodingOpts encoding.Options
	// ReaderIteratorPool yields ReaderIterators.
	ReaderIteratorPool encoding.ReaderIteratorPool
}

// SeriesBlockFrame contains either all raw values
// for a given series in a block if the frame size
// was not specified, or the number of values
// that fall into the next sequential frame
// for a series in the block given the progression
// through each time series from the query Start time.
// e.g. with 10minute frame size that aligns with the
// query start, each series will return
// 12 frames in a two hour block.
type SeriesBlockFrame struct {
	// FrameStartInclusive is inclusive start of frame.
	FrameStartInclusive xtime.UnixNano
	// FrameEndExclusive is exclusive end of frame.
	FrameEndExclusive xtime.UnixNano
	// recorder is the recorder.
	recorder *recorder
}
