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

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

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
	// FrameStart is start of frame.
	FrameStart xtime.UnixNano
	// FrameEnd is end of frame.
	FrameEnd xtime.UnixNano
	// datapointRecord is the record.
	record *record
	// tags are the tags for the current series.
	tags ident.TagIterator
	// id is the id for this series.
	id ident.ID
}

func (f *SeriesBlockFrame) release() {
	f.record.release()
}

func (f *SeriesBlockFrame) reset(
	start xtime.UnixNano,
	end xtime.UnixNano,
	id ident.ID,
	tags ident.TagIterator,
) {
	f.release()
	f.FrameStart = start
	f.FrameEnd = end
	f.id = id
	f.tags = tags
}

// Values returns values in this SeriesBlockFrame.
func (f *SeriesBlockFrame) Values() []float64 {
	return f.record.values()
}

// Sum returns the sum of values in this SeriesBlockFrame.
func (f *SeriesBlockFrame) Sum() float64 {
	return f.record.sum()
}

// Timestamps returns timestamps for the SeriesBlockFrame.
func (f *SeriesBlockFrame) Timestamps() []time.Time {
	return f.record.timestamps()
}

// Units returns units for the SeriesBlockFrame.
func (f *SeriesBlockFrame) Units() SeriesFrameUnits {
	return f.record.units
}

// Annotations returns annotations for the SeriesBlockFrame.
func (f *SeriesBlockFrame) Annotations() SeriesFrameAnnotations {
	return f.record.annotations
}

// Tags returns the tags for this SeriesBlockFrame.
func (f *SeriesBlockFrame) Tags() ident.TagIterator {
	return f.tags
}

// ID returns the ID for this SeriesBlockFrame.
func (f *SeriesBlockFrame) ID() ident.ID {
	return f.id
}
