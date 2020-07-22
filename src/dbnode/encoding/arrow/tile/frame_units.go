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
	xtime "github.com/m3db/m3/src/x/time"
)

type unitRecorder struct {
	set   bool
	count int
	u     xtime.Unit
	us    []xtime.Unit
}

var _ SeriesFrameUnits = (*unitRecorder)(nil)

func newUnitRecorder() *unitRecorder {
	return &unitRecorder{}
}

func (u *unitRecorder) SingleValue() (xtime.Unit, bool) {
	return u.u, u.set && len(u.us) == 0
}

func (u *unitRecorder) Values() []xtime.Unit {
	if len(u.us) == 0 {
		if u.us == nil {
			u.us = make([]xtime.Unit, 0, u.count)
		}

		for i := 0; i < u.count; i++ {
			u.us = append(u.us, u.u)
		}
	}

	return u.us
}

func (u *unitRecorder) record(unit xtime.Unit) {
	u.count++
	if !u.set {
		u.set = true
		u.u = unit
		return
	}

	// NB: unit has already changed in this dataset.
	if len(u.us) > 0 {
		u.us = append(u.us, unit)
		return
	}

	// NB: same unit as previously recorded; skip.
	if u.u == unit {
		return
	}

	if u.us == nil {
		u.us = make([]xtime.Unit, 0, u.count)
	}

	for i := 0; i < u.count-1; i++ {
		u.us = append(u.us, u.u)
	}

	u.us = append(u.us, unit)
}

func (u *unitRecorder) reset() {
	u.count = 0
	u.set = false
	u.us = u.us[:0]
}
