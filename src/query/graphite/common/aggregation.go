package common

import (
	"math"
	"time"

	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/graphite/ts"
)

// Combine combines multiple series into a single series using a consolidation
// function. If the series use different time intervals, the coarsest interval
// will apply.
func Combine(
	ctx *Context, in ts.SeriesList, fn ts.ConsolidationFunc, renamer SeriesListRenamer) (*ts.Series, error) {
	if in.Len() == 0 {
		return nil, ErrEmptySeriesList
	}

	var (
		start, end    = in.Values[0].StartTime(), in.Values[0].EndTime()
		millisPerStep = 0
	)

	for _, series := range in.Values {
		if series.StartTime().Before(start) {
			start = series.StartTime()
		}

		if series.EndTime().After(end) {
			end = series.EndTime()
		}

		if series.MillisPerStep() > millisPerStep {
			millisPerStep = series.MillisPerStep()
		}
	}

	var (
		consolidation = ts.NewConsolidation(ctx, start, end, millisPerStep, fn)
		cf            ts.ConsolidationFunc
	)
	for _, series := range in.Values {
		// All the series should have the same consolidation function. I could not
		// think of any cases that 2 series with different consolidation functions
		// (2 series with different metric types) should be consolidated together.
		if cf == nil {
			if !series.IsConsolidationFuncSet() {
				cf = graphite.FindRetentionPolicy(series.Name(),
					time.Since(series.StartTime())).Consolidation.Func()
			} else {
				cf = series.ConsolidationFunc()
			}
		}

		if !series.IsConsolidationFuncSet() {
			series.SetConsolidationFunc(cf)
		}

		consolidation.AddSeries(series, ts.Avg)
	}

	result := consolidation.BuildSeries(renamer(in), ts.Finalize)
	result.SetConsolidationFunc(cf)

	return result, nil
}

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
