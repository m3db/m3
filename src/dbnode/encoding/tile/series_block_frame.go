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

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// NB: 2 hr block / 15 sec scrape as an initial size.
	initLength = 2 * 60 * 4
)

type recorder struct {
	vals  []float64
	times []time.Time // todo: consider delta-delta here?

	units       *unitRecorder
	annotations *annotationRecorder
}

func newRecorder() *recorder {
	return &recorder{
		vals:  make([]float64, 0, initLength),
		times: make([]time.Time, 0, initLength),

		units:       newUnitRecorder(),
		annotations: newAnnotationRecorder(),
	}
}

func (r *recorder) reset() {
	r.units.reset()
	r.annotations.reset()
	r.vals = r.vals[:0]
	r.times = r.times[:0]
}

func (r *recorder) record(dp ts.Datapoint, u xtime.Unit, a ts.Annotation) {
	r.vals = append(r.vals, dp.Value)
	r.times = append(r.times, dp.Timestamp)
	r.units.record(u)
	r.annotations.record(a)
}

func newSeriesBlockFrame(recorder *recorder) SeriesBlockFrame {
	return SeriesBlockFrame{recorder: recorder}
}

func (f *SeriesBlockFrame) reset(
	start xtime.UnixNano,
	end xtime.UnixNano,
) {
	f.recorder.reset()
	f.FrameStartInclusive = start
	f.FrameEndExclusive = end
}

func (f *SeriesBlockFrame) record(dp ts.Datapoint, u xtime.Unit, a ts.Annotation) {
	f.recorder.record(dp, u, a)
}

// Values returns values in this SeriesBlockFrame.
func (f *SeriesBlockFrame) Values() []float64 {
	return f.recorder.vals
}

// Timestamps returns timestamps for the SeriesBlockFrame.
func (f *SeriesBlockFrame) Timestamps() []time.Time {
	return f.recorder.times
}

// Units returns units for the SeriesBlockFrame.
func (f *SeriesBlockFrame) Units() SeriesFrameUnits {
	return f.recorder.units
}

// Annotations returns annotations for the SeriesBlockFrame.
func (f *SeriesBlockFrame) Annotations() SeriesFrameAnnotations {
	return f.recorder.annotations
}
