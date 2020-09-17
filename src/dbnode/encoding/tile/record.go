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
)

type record struct {
	vals  []float64
	times []time.Time

	units       *unitRecorder
	annotations *annotationRecorder
}

func (r *record) values() []float64 {
	return r.vals
}

func (r *record) sum() float64 {
	sum := 0.0
	for _, v := range r.vals {
		sum += v
	}

	return sum
}

func (r *record) timestamps() []time.Time {
	return r.times
}

func (r *record) release() {
	if r == nil {
		return
	}

	if r.units != nil {
		r.units.reset()
	}

	if r.annotations != nil {
		r.annotations.reset()
	}

	r.vals = r.vals[:0]
	r.times = r.times[:0]
}

func (r *record) setFlatValues(vals []float64, times []time.Time) {
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
