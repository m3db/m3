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

	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/math"
)

type record struct {
	vals  []float64
	times []time.Time

	array.Record
	units       *unitRecorder
	annotations *annotationRecorder
}

func (r *record) values() []float64 {
	if r.vals != nil {
		return r.vals
	}

	return r.Columns()[valIdx].(*array.Float64).Float64Values()
}

func (r *record) sum() float64 {
	if len(r.vals) > 0 {
		sum := 0.0
		for _, v := range r.vals {
			sum += v
		}

		return sum
	}

	arr := r.Columns()[valIdx].(*array.Float64)
	return math.Float64.Sum(arr)
}

func (r *record) timestamps() []time.Time {
	if r.times != nil {
		return r.times
	}

	arrowTimes := r.Columns()[timeIdx].(*array.Int64).Int64Values()
	r.times = make([]time.Time, 0, len(arrowTimes))
	for _, t := range arrowTimes {
		r.times = append(r.times, time.Unix(0, t))
	}

	return r.times
}

func (r *record) release() {
	if r == nil {
		return
	}

	if r.Record != nil {
		r.Record.Release()
	}

	if r.units != nil {
		r.units.reset()
	}

	if r.annotations != nil {
		r.annotations.reset()
	}

	if r.vals != nil {
		r.vals = r.vals[:0]
	}

	if r.times != nil {
		r.times = r.times[:0]
	}

	r.Record = nil
}

func (r *record) setFlatValues(vals []float64, times []time.Time) {
	if r.Record != nil {
		r.Record.Release()
		r.Record = nil
	}

	r.vals = vals
	r.times = times
}

func (r *record) setUnitsAnnotations(
	units *unitRecorder, annotations *annotationRecorder) {
	r.units = units
	r.annotations = annotations
}

func newDatapointRecord() *record {
	return &record{}
}
