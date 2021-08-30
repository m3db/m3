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

package native

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/x/errors"
)

// summarize summarizes each series into interval buckets of a certain size.
func summarize(
	ctx *common.Context,
	series singlePathSpec,
	intervalS, fname string,
	alignToFrom bool,
) (ts.SeriesList, error) {
	if fname == "" {
		fname = "sum"
	}

	safeAggFn, ok := common.SafeAggregationFns[fname]
	if !ok {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf(
			"aggregate function not supported: %s", fname))
	}

	interval, err := common.ParseInterval(intervalS)
	if err != nil || interval <= 0 {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"invalid interval %s: %v", interval, err))
		return ts.NewSeriesList(), err
	}

	alignString := ""
	if alignToFrom {
		alignString = ", true"
	}

	results := make([]*ts.Series, len(series.Values))
	for i, series := range series.Values {
		name := fmt.Sprintf("summarize(%s, %q, %q%s)", series.Name(), intervalS, fname, alignString)
		results[i] = summarizeTimeSeries(ctx, name, series, interval, safeAggFn, alignToFrom)
	}

	r := ts.SeriesList(series)
	r.Values = results
	return r, nil
}

type summarizeBucket struct {
	vals []float64
}

func summarizeTimeSeries(
	ctx *common.Context,
	newName string,
	series *ts.Series,
	interval time.Duration,
	safeAggFn common.SafeAggregationFn,
	alignToFrom bool,
) *ts.Series {
	var (
		startTimeInSecs = int(series.StartTime().Unix())
		intervalInSecs  = int(interval / time.Second)
		intervalInMsecs = intervalInSecs * 1000
		buckets         = make(map[int]*summarizeBucket)
	)

	for i := 0; i < series.Len(); i++ {
		timestamp, n := int(series.StartTimeForStep(i).Unix()), series.ValueAt(i)
		if math.IsNaN(n) {
			continue
		}

		bucketInterval := timestamp - (timestamp % intervalInSecs)
		if alignToFrom {
			bucketInterval = (timestamp - startTimeInSecs) / intervalInSecs
		}

		if bucket, exists := buckets[bucketInterval]; exists {
			bucket.vals = append(bucket.vals, n)
		} else {
			buckets[bucketInterval] = &summarizeBucket{[]float64{n}}
		}
	}

	var (
		newStart = series.StartTime()
		newEnd   = series.EndTime()
	)

	if !alignToFrom {
		newStartInSecs, newEndInSecs := newStart.Unix(), newEnd.Unix()
		newStart = time.Unix(newStartInSecs-newStartInSecs%int64(intervalInSecs), 0)
		newEnd = time.Unix(newEndInSecs-newEndInSecs%int64(intervalInSecs)+int64(intervalInSecs), 0)
	}

	var (
		numSteps  = ts.NumSteps(newStart, newEnd, intervalInMsecs)
		newValues = ts.NewValues(ctx, intervalInMsecs, numSteps)
	)

	for timestamp, i := newStart, 0; i < newValues.Len(); timestamp, i = timestamp.Add(interval), i+1 {
		timestampInSecs := int(timestamp.Unix())
		var bucketInterval int
		if alignToFrom {
			bucketInterval = (timestampInSecs - startTimeInSecs) / intervalInSecs
		} else {
			bucketInterval = timestampInSecs - (timestampInSecs % intervalInSecs)
		}

		bucket, bucketExists := buckets[bucketInterval]
		if bucketExists {
			safeValue, _, safe := safeAggFn(bucket.vals)
			if safe {
				newValues.SetValueAt(i, safeValue)
			}
		}
	}
	return ts.NewSeries(ctx, newName, newStart, newValues)
}

// smartSummarize is an alias of summarize with alignToFrom set to true
func smartSummarize(
	ctx *common.Context,
	series singlePathSpec,
	interval, fname string,
) (ts.SeriesList, error) {
	alignToFrom := true

	seriesList, err := summarize(ctx, series, interval, fname, alignToFrom)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	results := seriesList.Values
	for i, series := range seriesList.Values {
		oldName := series.Name()
		newName := strings.Replace(oldName, "summarize", "smartSummarize", 1)
		newName = strings.Replace(newName, ", true", "", 1)
		results[i] = series.RenamedTo(newName)
	}

	// Retain whether sort was applied or not and metadata.
	r := ts.SeriesList(series)
	r.Values = results
	return r, nil
}

// specificationFunc determines the output series specification given a series list.
type specificationFunc func(ts.SeriesList) string

func sumSpecificationFunc(series ts.SeriesList) string {
	return wrapPathExpr("sumSeries", series)
}

func averageSpecificationFunc(series ts.SeriesList) string {
	return wrapPathExpr("averageSeries", series)
}

func multiplyWithWildcardsSpecificationFunc(series ts.SeriesList) string {
	return wrapPathExpr("multiplySeriesWithWildcards", series)
}
