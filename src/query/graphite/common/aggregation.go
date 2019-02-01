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

package common

import (
	"math"

	"github.com/m3db/m3/src/query/graphite/ts"
)

// Range distills down a set of inputs into the range of the series.
func Range(ctx *Context, series ts.SeriesList, renamer SeriesListRenamer) (*ts.Series, error) {
	numSeries := series.Len()
	if numSeries == 0 {
		return nil, ErrEmptySeriesList
	}
	normalized, start, end, millisPerStep, err := Normalize(ctx, series)
	if err != nil {
		return nil, err
	}
	numSteps := ts.NumSteps(start, end, millisPerStep)
	vals := ts.NewValues(ctx, millisPerStep, numSteps)
	nan := math.NaN()

	for i := 0; i < numSteps; i++ {
		minVal, maxVal := nan, nan
		for j := 0; j < numSeries; j++ {
			v := normalized.Values[j].ValueAt(i)
			if math.IsNaN(v) {
				continue
			}
			if math.IsNaN(minVal) || minVal > v {
				minVal = v
			}
			if math.IsNaN(maxVal) || maxVal < v {
				maxVal = v
			}
		}
		if !math.IsNaN(minVal) && !math.IsNaN(maxVal) {
			vals.SetValueAt(i, maxVal-minVal)
		}
	}
	name := renamer(normalized)
	return ts.NewSeries(ctx, name, start, vals), nil
}
