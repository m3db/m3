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

package storage

import (
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/storage"
)

// ConvertToGraphiteTS converts a
func ConvertToGraphiteTS(
	ctx context.Context,
	start time.Time,
	fetched *storage.FetchResult,
) (*FetchResult, error) {
	list := fetched.SeriesList
	seriesList := make([]*ts.Series, len(list))
	for i, series := range list {
		name := series.Name()

		// FIXME: add in the millis per step for this series
		vals := ts.NewValues(ctx, 0, series.Len())
		dps := series.Values().Datapoints()
		for i, dp := range dps {
			vals.SetValueAt(i, dp.Value)
		}

		seriesList[i] = ts.NewSeries(ctx, name, start, vals)
	}

	return NewFetchResult(ctx, seriesList), nil
}
