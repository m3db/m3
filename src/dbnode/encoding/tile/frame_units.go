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
	"fmt"

	xtime "github.com/m3db/m3/src/x/time"
)

type unitRecorder struct {
	count int
	u     xtime.Unit
	us    []xtime.Unit
}

var _ SeriesFrameUnits = (*unitRecorder)(nil)

func newUnitRecorder() *unitRecorder {
	return &unitRecorder{}
}

func (r *unitRecorder) Value(idx int) (xtime.Unit, error) {
	if idx < 0 || idx >= r.count {
		return 0, fmt.Errorf("unitRecorder.Value index (%d) out of bounds [0; %d)", idx, r.count)
	}

	if r.singleValue() {
		return r.u, nil
	}

	return r.us[idx], nil
}

func (r *unitRecorder) singleValue() bool {
	return r.count > 0 && len(r.us) == 0
}

func (r *unitRecorder) record(unit xtime.Unit) {
	r.count++
	if r.count == 1 {
		r.u = unit
		return
	}

	// NB: unit has already changed in this dataset.
	if len(r.us) > 0 {
		r.us = append(r.us, unit)
		return
	}

	// NB: same unit as previously recorded; skip.
	if r.u == unit {
		return
	}

	if r.us == nil {
		r.us = make([]xtime.Unit, 0, r.count)
	}

	for i := 0; i < r.count-1; i++ {
		r.us = append(r.us, r.u)
	}

	r.us = append(r.us, unit)
}

func (r *unitRecorder) reset() {
	r.count = 0
	r.us = r.us[:0]
}
