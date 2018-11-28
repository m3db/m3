// Copyright (c) 2018 Uber Technologies, Inc.
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

package fanout

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
)

func datapointsToBlocks(
	datapoints []ts.Datapoints,
	tagOptions models.TagOptions,
	start, end time.Time,
	stepSize time.Duration,
) (block.UnconsolidatedBlock, error) {
	seriesList := make(ts.SeriesList, len(datapoints))
	for i, dp := range datapoints {
		// TODO: consider propagating series names and tags?
		seriesList[i] = ts.NewSeries(
			fmt.Sprintf("series_%d", i),
			dp,
			models.Tags{
				Opts: tagOptions,
				Tags: []models.Tag{{
					Name:  []byte("series_number"),
					Value: []byte(fmt.Sprintf("%d", i)),
				}},
			},
		)
	}

	fetchQuery := &storage.FetchQuery{
		Start:    start,
		End:      end,
		Interval: stepSize,
	}

	return storage.NewMultiSeriesBlock(seriesList, fetchQuery)
}
