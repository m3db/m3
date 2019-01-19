package native

import (
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/ts"
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
		return ts.SeriesList{}, err
	}

	f, fexists := summarizeFuncs[fname]
	if !fexists {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"invalid func %s", fname))
		return ts.SeriesList{}, err
	}

	alignString := ""
	if alignToFrom {
		alignString = ", true"
	}

	results := make([]*ts.Series, len(series.Values))
	for i, series := range series.Values {
		name := fmt.Sprintf("summarize(%s, \"%s\", \"%s\"%s)", series.Name(), intervalS, fname, alignString)
		results[i] = summarizeTimeSeries(ctx, name, series, interval, f.consolidationFunc, alignToFrom)
	}

	r := ts.SeriesList(series)
	r.Values = results
	return r, nil
}

type summarizeBucket struct {
	count int
	accum float64
}

func summarizeTimeSeries(
	ctx *common.Context,
	newName string,
	series *ts.Series,
	interval time.Duration,
	f ts.ConsolidationFunc,
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
			bucket.accum = f(bucket.accum, n, bucket.count)
			bucket.count++
		} else {
			buckets[bucketInterval] = &summarizeBucket{1, n}
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
			newValues.SetValueAt(i, bucket.accum)
		}
	}
	return ts.NewSeries(ctx, newName, newStart, newValues)
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

type funcInfo struct {
	consolidationFunc ts.ConsolidationFunc
	specificationFunc specificationFunc
}

var (
	sumFuncInfo  = funcInfo{ts.Sum, sumSpecificationFunc}
	maxFuncInfo  = funcInfo{ts.Max, maxSpecificationFunc}
	minFuncInfo  = funcInfo{ts.Min, minSpecificationFunc}
	lastFuncInfo = funcInfo{ts.Last, lastSpecificationFunc}
	avgFuncInfo  = funcInfo{ts.Avg, averageSpecificationFunc}

	summarizeFuncs = map[string]funcInfo{
		"sum":           sumFuncInfo,
		"max":           maxFuncInfo,
		"min":           minFuncInfo,
		"last":          lastFuncInfo,
		"avg":           avgFuncInfo,
		"sumSeries":     sumFuncInfo,
		"maxSeries":     maxFuncInfo,
		"minSeries":     minFuncInfo,
		"averageSeries": avgFuncInfo,
		"":              sumFuncInfo,
	}
)
