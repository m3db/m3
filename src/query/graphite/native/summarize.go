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

	interval, err := common.ParseInterval(intervalS)
	if err != nil || interval <= 0 {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"invalid interval %s: %v", interval, err))
		return ts.NewSeriesList(), err
	}

	f, fexists := summarizeFuncs[fname]
	if !fexists {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"invalid func %s", fname))
		return ts.NewSeriesList(), err
	}

	alignString := ""
	if alignToFrom {
		alignString = ", true"
	}

	results := make([]*ts.Series, len(series.Values))
	for i, series := range series.Values {
		name := fmt.Sprintf("summarize(%s, \"%s\", \"%s\"%s)", series.Name(), intervalS, fname, alignString)
		results[i] = summarizeTimeSeries(ctx, name, series, interval, f, alignToFrom)
	}

	r := ts.SeriesList(series)
	r.Values = results
	return r, nil
}

type summarizeBucket struct {
	count int
	accum float64
	vals  []float64
}

func summarizeTimeSeries(
	ctx *common.Context,
	newName string,
	series *ts.Series,
	interval time.Duration,
	funcInfo funcInfo,
	alignToFrom bool,
) *ts.Series {
	var (
		startTimeInSecs = int(series.StartTime().Unix())
		intervalInSecs  = int(interval / time.Second)
		intervalInMsecs = intervalInSecs * 1000
		buckets         = make(map[int]*summarizeBucket)
		f               = funcInfo.consolidationFunc
		fname           = funcInfo.fname
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
			if fname == "median" {
				bucket.vals = append(bucket.vals, n)
			} else {
				bucket.accum = f(bucket.accum, n, bucket.count)
			}
			bucket.count++
		} else {
			vals := []float64{n}
			buckets[bucketInterval] = &summarizeBucket{1, n, vals}
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
			if fname == "median" {
				newValues.SetValueAt(i, ts.Median(bucket.vals, bucket.count))
			} else {
				newValues.SetValueAt(i, bucket.accum)
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

func maxSpecificationFunc(series ts.SeriesList) string {
	return wrapPathExpr("maxSeries", series)
}

func minSpecificationFunc(series ts.SeriesList) string {
	return wrapPathExpr("minSeries", series)
}

func lastSpecificationFunc(series ts.SeriesList) string {
	return wrapPathExpr("lastSeries", series)
}

func medianSpecificationFunc(series ts.SeriesList) string {
	return wrapPathExpr("medianSeries", series)
}

type funcInfo struct {
	fname             string
	consolidationFunc ts.ConsolidationFunc
	specificationFunc specificationFunc
}

var (
	sumFuncInfo = funcInfo{
		fname:             "sum",
		consolidationFunc: ts.Sum,
		specificationFunc: sumSpecificationFunc,
	}
	maxFuncInfo = funcInfo{
		fname:             "max",
		consolidationFunc: ts.Max,
		specificationFunc: maxSpecificationFunc,
	}
	minFuncInfo = funcInfo{
		fname:             "min",
		consolidationFunc: ts.Min,
		specificationFunc: minSpecificationFunc,
	}
	lastFuncInfo = funcInfo{
		fname:             "last",
		consolidationFunc: ts.Last,
		specificationFunc: lastSpecificationFunc,
	}
	avgFuncInfo = funcInfo{
		fname:             "avg",
		consolidationFunc: ts.Avg,
		specificationFunc: averageSpecificationFunc,
	}
	medianFuncInfo = funcInfo{
		fname: "median",
		// median does not have a consolidationFunc
		specificationFunc: medianSpecificationFunc,
	}

	summarizeFuncs = map[string]funcInfo{
		"sum":           sumFuncInfo,
		"max":           maxFuncInfo,
		"min":           minFuncInfo,
		"last":          lastFuncInfo,
		"avg":           avgFuncInfo,
		"average":       avgFuncInfo,
		"sumSeries":     sumFuncInfo,
		"maxSeries":     maxFuncInfo,
		"median":        medianFuncInfo,
		"minSeries":     minFuncInfo,
		"averageSeries": avgFuncInfo,
		"":              sumFuncInfo,
	}
)
