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

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/ts"
)

func wrapPathExpr(wrapper string, series ts.SeriesList) string {
	return fmt.Sprintf("%s(%s)", wrapper, joinPathExpr(series))
}

// sumSeries adds metrics together and returns the sum at each datapoint.
// If the time series have different intervals, the coarsest interval will be used.
func sumSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr("sumSeries", ts.SeriesList(series)), ts.Sum)
}

// diffSeries subtracts all but the first series from the first series.
// If the time series have different intervals, the coarsest interval will be used.
func diffSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	transformedSeries := series
	numSeries := len(series.Values)
	if numSeries > 1 {
		transformedSeries.Values = make([]*ts.Series, numSeries)
		transformedSeries.Values[0] = series.Values[0]
		for i := 1; i < len(series.Values); i++ {
			res, err := transform(
				ctx,
				singlePathSpec{Values: []*ts.Series{series.Values[i]}},
				func(n string) string { return n },
				common.MaintainNaNTransformer(func(v float64) float64 { return -v }),
			)
			if err != nil {
				return ts.SeriesList{}, err
			}
			transformedSeries.Values[i] = res.Values[0]
		}
	}

	return combineSeries(ctx, transformedSeries, wrapPathExpr("diffSeries", ts.SeriesList(series)), ts.Sum)
}

// multiplySeries multiplies metrics together and returns the product at each datapoint.
// If the time series have different intervals, the coarsest interval will be used.
func multiplySeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr("multiplySeries", ts.SeriesList(series)), ts.Mul)
}

// averageSeries takes a list of series and returns a new series containing the
// average of all values at each datapoint.
func averageSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr("averageSeries", ts.SeriesList(series)), ts.Avg)
}

// minSeries takes a list of series and returns a new series containing the
// minimum value across the series at each datapoint
func minSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr("minSeries", ts.SeriesList(series)), ts.Min)
}

// maxSeries takes a list of series and returns a new series containing the
// maximum value across the series at each datapoint
func maxSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr("maxSeries", ts.SeriesList(series)), ts.Max)
}

// divideSeries divides one series list by another series
func divideSeries(ctx *common.Context, dividendSeriesList, divisorSeriesList singlePathSpec) (ts.SeriesList, error) {
	if len(divisorSeriesList.Values) != 1 {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"divideSeries second argument must reference exactly one series but instead has %d",
			len(divisorSeriesList.Values)))
		return ts.SeriesList{}, err
	}
	if len(dividendSeriesList.Values) == 0 {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"divideSeries first argument must reference at least one series"))
		return ts.SeriesList{}, err
	}

	divisorSeries := divisorSeriesList.Values[0]
	results := make([]*ts.Series, len(dividendSeriesList.Values))
	for idx, dividendSeries := range dividendSeriesList.Values {
		normalized, minBegin, _, lcmMillisPerStep, err := common.Normalize(ctx, ts.SeriesList{
			Values: []*ts.Series{dividendSeries, divisorSeries},
		})
		if err != nil {
			return ts.SeriesList{}, err
		}
		// NB(bl): Normalized must give back exactly two series of the same length.
		dividend, divisor := normalized.Values[0], normalized.Values[1]
		numSteps := dividend.Len()
		vals := ts.NewValues(ctx, lcmMillisPerStep, numSteps)
		for i := 0; i < numSteps; i++ {
			dividendVal := dividend.ValueAt(i)
			divisorVal := divisor.ValueAt(i)
			if !math.IsNaN(dividendVal) && !math.IsNaN(divisorVal) && divisorVal != 0 {
				value := dividendVal / divisorVal
				vals.SetValueAt(i, value)
			}
		}
		name := fmt.Sprintf("divideSeries(%s,%s)", dividend.Name(), divisor.Name())
		quotientSeries := ts.NewSeries(ctx, name, minBegin, vals)
		results[idx] = quotientSeries
	}

	r := ts.SeriesList(dividendSeriesList)
	r.Values = results
	return r, nil
}

// averageSeriesWithWildcards splits the given set of series into sub-groupings
// based on wildcard matches in the hierarchy, then averages the values in each
// grouping
func averageSeriesWithWildcards(
	ctx *common.Context,
	series singlePathSpec,
	positions ...int,
) (ts.SeriesList, error) {
	return combineSeriesWithWildcards(ctx, series, positions, averageSpecificationFunc, ts.Avg)
}

// sumSeriesWithWildcards splits the given set of series into sub-groupings
// based on wildcard matches in the hierarchy, then sums the values in each
// grouping
func sumSeriesWithWildcards(
	ctx *common.Context,
	series singlePathSpec,
	positions ...int,
) (ts.SeriesList, error) {
	return combineSeriesWithWildcards(ctx, series, positions, sumSpecificationFunc, ts.Sum)
}

// combineSeriesWithWildcards splits the given set of series into sub-groupings
// based on wildcard matches in the hierarchy, then combines the values in each
// sub-grouping according to the provided consolidation function
func combineSeriesWithWildcards(
	ctx *common.Context,
	series singlePathSpec,
	positions []int,
	sf specificationFunc,
	f ts.ConsolidationFunc,
) (ts.SeriesList, error) {
	if len(series.Values) == 0 {
		return ts.SeriesList(series), nil
	}

	var (
		toCombine = make(map[string][]*ts.Series)
		wildcards = make(map[int]struct{})
	)

	for _, position := range positions {
		wildcards[position] = struct{}{}
	}

	for _, series := range series.Values {
		var (
			parts    = strings.Split(series.Name(), ".")
			newParts = make([]string, 0, len(parts))
		)
		for i, part := range parts {
			if _, wildcard := wildcards[i]; !wildcard {
				newParts = append(newParts, part)
			}
		}

		newName := strings.Join(newParts, ".")
		toCombine[newName] = append(toCombine[newName], series)
	}

	newSeries := make([]*ts.Series, 0, len(toCombine))
	for name, series := range toCombine {
		seriesList := ts.SeriesList{Values: series}
		combined, err := combineSeries(ctx, multiplePathSpecs(seriesList), name, f)
		if err != nil {
			return ts.SeriesList{}, err
		}
		combined.Values[0].Specification = sf(seriesList)
		newSeries = append(newSeries, combined.Values...)
	}

	r := ts.SeriesList(series)

	r.Values = newSeries

	// Ranging over hash map to create results destroys
	// any sort order on the incoming series list
	r.SortApplied = false

	return r, nil
}

// groupByNode takes a serieslist and maps a callback to subgroups within as defined by a common node
//
//    &target=groupByNode(foo.by-function.*.*.cpu.load5,2,"sumSeries")
//
//  Would return multiple series which are each the result of applying the "sumSeries" function
//  to groups joined on the second node (0 indexed) resulting in a list of targets like
//
//    sumSeries(foo.by-function.server1.*.cpu.load5),sumSeries(foo.by-function.server2.*.cpu.load5),...
func groupByNode(ctx *common.Context, series singlePathSpec, node int, fname string) (ts.SeriesList, error) {
	metaSeries := make(map[string][]*ts.Series)
	for _, s := range series.Values {
		parts := strings.Split(s.Name(), ".")

		n := node
		if n < 0 {
			n = len(parts) + n
		}

		if n >= len(parts) || n < 0 {
			err := errors.NewInvalidParamsError(fmt.Errorf("could not group %s by node %d; not enough parts", s.Name(), node))
			return ts.SeriesList{}, err
		}

		key := parts[n]
		metaSeries[key] = append(metaSeries[key], s)
	}

	if fname == "" {
		fname = "sum"
	}

	f, fexists := summarizeFuncs[fname]
	if !fexists {
		return ts.SeriesList{}, errors.NewInvalidParamsError(fmt.Errorf("invalid func %s", fname))
	}

	newSeries := make([]*ts.Series, 0, len(metaSeries))
	for key, series := range metaSeries {
		seriesList := ts.SeriesList{Values: series}
		output, err := combineSeries(ctx, multiplePathSpecs(seriesList), key, f.consolidationFunc)
		if err != nil {
			return ts.SeriesList{}, err
		}
		output.Values[0].Specification = f.specificationFunc(seriesList)
		newSeries = append(newSeries, output.Values...)
	}

	r := ts.SeriesList(series)

	r.Values = newSeries

	// Ranging over hash map to create results destroys
	// any sort order on the incoming series list
	r.SortApplied = false

	return r, nil
}

// combineSeries combines multiple series into a single series using a
// consolidation func.  If the series use different time intervals, the
// coarsest time will apply.
func combineSeries(ctx *common.Context,
	series multiplePathSpecs,
	fname string,
	f ts.ConsolidationFunc,
) (ts.SeriesList, error) {
	if len(series.Values) == 0 { // no data; no work
		return ts.SeriesList(series), nil
	}

	normalized, start, end, millisPerStep, err := common.Normalize(ctx, ts.SeriesList(series))
	if err != nil {
		err := errors.NewInvalidParamsError(fmt.Errorf("combine series error: %v", err))
		return ts.SeriesList{}, err
	}

	consolidation := ts.NewConsolidation(ctx, start, end, millisPerStep, f)
	for _, s := range normalized.Values {
		consolidation.AddSeries(s, ts.Avg)
	}

	result := consolidation.BuildSeries(fname, ts.Finalize)
	return ts.SeriesList{Values: []*ts.Series{result}}, nil
}

// weightedAverage takes a series of values and a series of weights and produces a weighted
// average for all values. The corresponding values should share a node as defined by the
// node parameter, 0-indexed.
func weightedAverage(
	ctx *common.Context,
	input singlePathSpec,
	weights singlePathSpec,
	node int,
) (ts.SeriesList, error) {
	step := math.MaxInt32
	if len(input.Values) > 0 {
		step = input.Values[0].MillisPerStep()
	} else if len(weights.Values) > 0 {
		step = weights.Values[0].MillisPerStep()
	} else {
		return ts.SeriesList(input), nil
	}

	for _, series := range input.Values {
		if step != series.MillisPerStep() {
			err := errors.NewInvalidParamsError(fmt.Errorf("different step sizes in input series not supported"))
			return ts.SeriesList{}, err
		}
	}

	for _, series := range weights.Values {
		if step != series.MillisPerStep() {
			err := errors.NewInvalidParamsError(fmt.Errorf("different step sizes in input series not supported"))
			return ts.SeriesList{}, err
		}
	}

	valuesByKey, err := aliasByNode(ctx, input, node)
	if err != nil {
		return ts.SeriesList{}, err
	}
	weightsByKey, err := aliasByNode(ctx, weights, node)
	if err != nil {
		return ts.SeriesList{}, err
	}

	type pairedSeries struct {
		values  *ts.Series
		weights *ts.Series
	}

	keys := make(map[string]*pairedSeries, len(valuesByKey.Values))

	for _, series := range valuesByKey.Values {
		keys[series.Name()] = &pairedSeries{values: series}
	}

	for _, series := range weightsByKey.Values {
		if tuple, ok := keys[series.Name()]; ok {
			tuple.weights = series
		}
	}

	productSeries := make([]*ts.Series, 0, len(keys))
	consideredWeights := make([]*ts.Series, 0, len(keys))

	for key, pair := range keys {

		if pair.weights == nil {
			continue // skip - no associated weight series
		}

		vals := ts.NewValues(ctx, pair.values.MillisPerStep(), pair.values.Len())
		for i := 0; i < pair.values.Len(); i++ {
			v := pair.values.ValueAt(i)
			w := pair.weights.ValueAt(i)
			vals.SetValueAt(i, v*w)
		}
		series := ts.NewSeries(ctx, key, pair.values.StartTime(), vals)
		productSeries = append(productSeries, series)
		consideredWeights = append(consideredWeights, pair.weights)
	}

	top, err := sumSeries(ctx, multiplePathSpecs(ts.SeriesList{
		Values: productSeries,
	}))
	if err != nil {
		return ts.SeriesList{}, err
	}

	bottom, err := sumSeries(ctx, multiplePathSpecs(ts.SeriesList{
		Values: consideredWeights,
	}))
	if err != nil {
		return ts.SeriesList{}, err
	}

	results, err := divideSeries(ctx, singlePathSpec(top), singlePathSpec(bottom))
	if err != nil {
		return ts.SeriesList{}, err
	}

	return alias(ctx, singlePathSpec(results), "weightedAverage")
}

// countSeries draws a horizontal line representing the number of nodes found in the seriesList.
func countSeries(ctx *common.Context, seriesList multiplePathSpecs) (ts.SeriesList, error) {
	count, err := common.Count(ctx, ts.SeriesList(seriesList), func(series ts.SeriesList) string {
		return wrapPathExpr("countSeries", series)
	})
	if err != nil {
		return ts.SeriesList{}, err
	}

	r := ts.SeriesList(seriesList)
	r.Values = count.Values
	return r, nil
}
