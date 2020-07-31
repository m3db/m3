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

// flatDatapointRecorder records datapoints without using arrow buffers.
type flatDatapointRecorder struct {
	vals        []float64
	times       []time.Time // todo: consider delta-delta here?
	units       *unitRecorder
	annotations *annotationRecorder
}

func (r *flatDatapointRecorder) record(dp ts.Datapoint, u xtime.Unit, a ts.Annotation) {
	r.vals = append(r.vals, dp.Value)
	r.times = append(r.times, dp.Timestamp)
	r.units.record(u)
	r.annotations.record(a)
}

var _ recorder = (*flatDatapointRecorder)(nil)

func newFlatDatapointRecorder() recorder {
	return &flatDatapointRecorder{
		vals:        make([]float64, 0, initLength),
		times:       make([]time.Time, 0, initLength),
		units:       newUnitRecorder(),
		annotations: newAnnotationRecorder(),
	}
}

func (r *flatDatapointRecorder) release() {
	r.units.reset()
	r.annotations.reset()
	if r.vals != nil {
		r.vals = r.vals[:0]
		r.vals = nil
	}

	if r.times != nil {
		r.times = r.times[:0]
		r.times = nil
	}
}

// NB: caller must release record.
func (r *flatDatapointRecorder) updateRecord(rec *record) {
	rec.units = r.units
	rec.annotations = r.annotations
	rec.vals = r.vals
	rec.times = r.times
	rec.Record = nil
}
