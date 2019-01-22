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
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"
)

// bootstrapWithIDs mocks fetches for now as the seriesList names are not actually IDs that are fetchable
// NaN vals will be returned for the period of startTime to EndTime
func bootstrapWithIDs(ctx *Context, seriesList ts.SeriesList, startTime, endTime time.Time) (ts.SeriesList, error) {
	bootstrapList := make([]*ts.Series, seriesList.Len())

	dur := int(endTime.Sub(startTime))
	for i, series := range seriesList.Values {
		numSteps := dur / (series.MillisPerStep() * 1000 * 1000) // convert to ns for step calculation
		vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
		bootstrapList[i] = ts.NewSeries(ctx, series.Name(), startTime, vals)
	}

	seriesList.Values = bootstrapList
	return seriesList, nil
}

// FetchWithBootstrap requests the same data but with a bootstrap period at the beginning.
func FetchWithBootstrap(ctx *Context, seriesList ts.SeriesList, duration time.Duration) (ts.SeriesList, error) {
	// Fetch bootstrapped series list between startTime and endTime
	startTime := ctx.StartTime.Add(-duration)
	endTime := ctx.StartTime
	bootstrapList, err := bootstrapWithIDs(ctx, seriesList, startTime, endTime)
	if err != nil {
		return ts.SeriesList{}, fmt.Errorf("unable to fetch bootstrap series, error=%s", err)
	}

	// Assemble the bootstrapped list
	newSeriesList := make([]*ts.Series, seriesList.Len())
	for i, bootstrap := range bootstrapList.Values {
		original := seriesList.Values[i]
		if bootstrap.MillisPerStep() < original.MillisPerStep() {
			bootstrap, err = bootstrap.IntersectAndResize(bootstrap.StartTime(), bootstrap.EndTime(), original.MillisPerStep(), original.ConsolidationFunc())
			if err != nil {
				return ts.SeriesList{}, err
			}
		}
		ratio := bootstrap.MillisPerStep() / original.MillisPerStep()
		numBootstrapValues := bootstrap.Len() * ratio
		numCombinedValues := numBootstrapValues + original.Len()
		values := ts.NewValues(ctx, original.MillisPerStep(), numCombinedValues)
		for j := 0; j < bootstrap.Len(); j++ {
			for k := j * ratio; k < (j+1)*ratio; k++ {
				values.SetValueAt(k, bootstrap.ValueAt(j))
			}
		}
		for j := numBootstrapValues; j < numCombinedValues; j++ {
			values.SetValueAt(j, original.ValueAt(j-numBootstrapValues))
		}
		newSeries := ts.NewSeries(ctx, original.Name(), startTime, values)
		newSeries.Specification = original.Specification
		newSeriesList[i] = newSeries
	}

	seriesList.Values = newSeriesList
	return seriesList, nil
}
