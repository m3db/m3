// Copyright (c) 2019 Uber Technologies, Inc.
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

package ts

import "fmt"

// SeriesReducerApproach defines an approach to reduce a series to a single value.
type SeriesReducerApproach string

// The standard set of reducers
const (
	SeriesReducerAvg    SeriesReducerApproach = "avg"
	SeriesReducerSum    SeriesReducerApproach = "sum"
	SeriesReducerMin    SeriesReducerApproach = "min"
	SeriesReducerMax    SeriesReducerApproach = "max"
	SeriesReducerStdDev SeriesReducerApproach = "stddev"
	SeriesReducerLast   SeriesReducerApproach = "last"

	SeriesReducerAverage SeriesReducerApproach = "average" // alias for "avg"
	SeriesReducerTotal   SeriesReducerApproach = "total"   // alias for "sum"
	SeriesReducerCurrent SeriesReducerApproach = "current" // alias for "last"
)

// SeriesReducer reduces a series to a single value.
type SeriesReducer func(*Series) float64

// SafeReducer returns a boolean indicating whether it is a valid reducer,
// and if so, the SeriesReducer implementing the SeriesReducerApproach.
func (sa SeriesReducerApproach) SafeReducer() (SeriesReducer, bool) {
	r, ok := seriesReducers[sa]
	return r, ok
}

// Reducer returns the SeriesReducer implementing the SeriesReducerApproach.
func (sa SeriesReducerApproach) Reducer() SeriesReducer {
	r, ok := sa.SafeReducer()
	if !ok {
		panic(fmt.Sprintf("No reducer func for %s", sa))
	}
	return r
}

var seriesReducers = map[SeriesReducerApproach]SeriesReducer{
	SeriesReducerAvg:     func(b *Series) float64 { return b.SafeAvg() },
	SeriesReducerAverage: func(b *Series) float64 { return b.SafeAvg() },
	SeriesReducerTotal:   func(b *Series) float64 { return b.SafeSum() },
	SeriesReducerSum:     func(b *Series) float64 { return b.SafeSum() },
	SeriesReducerMin:     func(b *Series) float64 { return b.SafeMin() },
	SeriesReducerMax:     func(b *Series) float64 { return b.SafeMax() },
	SeriesReducerStdDev:  func(b *Series) float64 { return b.SafeStdDev() },
	SeriesReducerLast:    func(b *Series) float64 { return b.SafeLastValue() },
	SeriesReducerCurrent: func(b *Series) float64 { return b.SafeLastValue() },
}
