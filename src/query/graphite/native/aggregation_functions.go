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
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
)

func wrapPathExpr(wrapper string, series ts.SeriesList) string {
	return fmt.Sprintf("%s(%s)", wrapper, joinPathExpr(series))
}

// sumSeries adds metrics together and returns the sum at each datapoint.
// If the time series have different intervals, the coarsest interval will be used.
func sumSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr(sumSeriesFnName, ts.SeriesList(series)), ts.Sum)
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
				return ts.NewSeriesList(), err
			}
			transformedSeries.Values[i] = res.Values[0]
		}
	}

	return combineSeries(ctx, transformedSeries, wrapPathExpr(diffSeriesFnName, ts.SeriesList(series)), ts.Sum)
}

// multiplySeries multiplies metrics together and returns the product at each datapoint.
// If the time series have different intervals, the coarsest interval will be used.
func multiplySeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr(multiplySeriesFnName, ts.SeriesList(series)), ts.Mul)
}

// averageSeries takes a list of series and returns a new series containing the
// average of all values at each datapoint.
func averageSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr(averageSeriesFnName, ts.SeriesList(series)), ts.Avg)
}

// minSeries takes a list of series and returns a new series containing the
// minimum value across the series at each datapoint
func minSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr(minSeriesFnName, ts.SeriesList(series)), ts.Min)
}

// powSeries takes a list of series and returns a new series containing the
// pow value across the series at each datapoint
// nolint: gocritic
func powSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr(powSeriesFnName, ts.SeriesList(series)), ts.Pow)
}

// maxSeries takes a list of series and returns a new series containing the
// maximum value across the series at each datapoint
func maxSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, wrapPathExpr(maxSeriesFnName, ts.SeriesList(series)), ts.Max)
}

// medianSeries takes a list of series and returns a new series containing the
// median value across the series at each datapoint
func medianSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	if len(series.Values) == 0 {
		return ts.NewSeriesList(), nil
	}
	normalized, start, end, millisPerStep, err := common.Normalize(ctx, ts.SeriesList(series))
	if err != nil {
		return ts.NewSeriesList(), err
	}
	numSteps := ts.NumSteps(start, end, millisPerStep)
	values := ts.NewValues(ctx, millisPerStep, numSteps)

	valuesAtTime := make([]float64, len(normalized.Values))
	for i := 0; i < numSteps; i++ {
		for j, series := range normalized.Values {
			valuesAtTime[j] = series.ValueAt(i)
		}
		values.SetValueAt(i, ts.Median(valuesAtTime, len(valuesAtTime)))
	}

	name := wrapPathExpr(medianSeriesFnName, ts.SeriesList(series))
	output := ts.NewSeries(ctx, name, start, values)
	return ts.SeriesList{
		Values:   []*ts.Series{output},
		Metadata: series.Metadata,
	}, nil
}

// lastSeries takes a list of series and returns a new series containing the
// last value at each datapoint
func lastSeries(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
	return combineSeries(ctx, series, joinPathExpr(ts.SeriesList(series)), ts.Last)
}

// standardDeviationHelper returns the standard deviation of a slice of a []float64
func standardDeviationHelper(values []float64) float64 {
	var count, sum float64

	for _, value := range values {
		if !math.IsNaN(value) {
			sum += value
			count++
		}
	}
	if count == 0 {
		return math.NaN()
	}
	avg := sum / count

	m2 := float64(0)
	for _, value := range values {
		if !math.IsNaN(value) {
			diff := value - avg
			m2 += diff * diff
		}
	}

	variance := m2 / count

	return math.Sqrt(variance)
}

// stddevSeries takes a list of series and returns a new series containing the
// standard deviation at each datapoint
// At step n, stddevSeries will make a list of every series' nth value,
// and calculate the standard deviation of that list.
// The output is a seriesList containing 1 series
func stddevSeries(ctx *common.Context, seriesList multiplePathSpecs) (ts.SeriesList, error) {
	if len(seriesList.Values) == 0 {
		return ts.NewSeriesList(), nil
	}

	firstSeries := seriesList.Values[0]
	numSteps := firstSeries.Len()
	values := ts.NewValues(ctx, firstSeries.MillisPerStep(), numSteps)
	valuesAtTime := make([]float64, 0, numSteps)
	for i := 0; i < numSteps; i++ {
		valuesAtTime = valuesAtTime[:0]
		for _, series := range seriesList.Values {
			if l := series.Len(); l != numSteps {
				return ts.NewSeriesList(), fmt.Errorf("mismatched series length, expected %d, got %d", numSteps, l)
			}
			valuesAtTime = append(valuesAtTime, series.ValueAt(i))
		}
		values.SetValueAt(i, standardDeviationHelper(valuesAtTime))
	}

	name := wrapPathExpr(stddevSeriesFnName, ts.SeriesList(seriesList))
	output := ts.NewSeries(ctx, name, firstSeries.StartTime(), values)
	return ts.SeriesList{
		Values:   []*ts.Series{output},
		Metadata: seriesList.Metadata,
	}, nil
}

func divideSeriesHelper(ctx *common.Context, dividendSeries, divisorSeries *ts.Series, metadata block.ResultMetadata) (*ts.Series, error) {
	normalized, minBegin, _, lcmMillisPerStep, err := common.Normalize(ctx, ts.SeriesList{
		Values:   []*ts.Series{dividendSeries, divisorSeries},
		Metadata: metadata,
	})
	if err != nil {
		return nil, err
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

	// The individual series will be named divideSeries(X, X), even if it is generated by divideSeriesLists
	// Based on Graphite source code (link below)
	// https://github.com/graphite-project/graphite-web/blob/17a34e7966f7a46eded30c2362765c74eea899cb/webapp/graphite/render/functions.py#L901
	name := fmt.Sprintf("divideSeries(%s,%s)", dividend.Name(), divisor.Name())
	quotientSeries := ts.NewSeries(ctx, name, minBegin, vals)
	return quotientSeries, nil
}

// divideSeries divides one series list by another single series
func divideSeries(ctx *common.Context, dividendSeriesList, divisorSeriesList singlePathSpec) (ts.SeriesList, error) {
	if len(dividendSeriesList.Values) == 0 || len(divisorSeriesList.Values) == 0 {
		return ts.NewSeriesList(), nil
	}
	if len(divisorSeriesList.Values) != 1 {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"divideSeries second argument must reference exactly one series but instead has %d",
			len(divisorSeriesList.Values)))
		return ts.NewSeriesList(), err
	}

	divisorSeries := divisorSeriesList.Values[0]
	results := make([]*ts.Series, len(dividendSeriesList.Values))
	for idx, dividendSeries := range dividendSeriesList.Values {
		metadata := divisorSeriesList.Metadata.CombineMetadata(dividendSeriesList.Metadata)
		quotientSeries, err := divideSeriesHelper(ctx, dividendSeries, divisorSeries, metadata)
		if err != nil {
			return ts.NewSeriesList(), err
		}
		results[idx] = quotientSeries
	}

	r := ts.SeriesList(dividendSeriesList)
	r.Values = results
	return r, nil
}

// divideSeriesLists divides one series list by another series list
func divideSeriesLists(ctx *common.Context, dividendSeriesList, divisorSeriesList singlePathSpec) (ts.SeriesList, error) {
	if len(dividendSeriesList.Values) != len(divisorSeriesList.Values) {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"divideSeriesLists both SeriesLists must have exactly the same length"))
		return ts.NewSeriesList(), err
	}
	results := make([]*ts.Series, len(dividendSeriesList.Values))
	for idx, dividendSeries := range dividendSeriesList.Values {
		divisorSeries := divisorSeriesList.Values[idx]
		metadata := divisorSeriesList.Metadata.CombineMetadata(dividendSeriesList.Metadata)
		quotientSeries, err := divideSeriesHelper(ctx, dividendSeries, divisorSeries, metadata)
		if err != nil {
			return ts.NewSeriesList(), err
		}
		results[idx] = quotientSeries
	}

	r := ts.SeriesList(dividendSeriesList)
	r.Values = results
	return r, nil
}

// aggregate takes a list of series and returns a new series containing the
// value aggregated across the series at each datapoint using the specified function.
// This function can be used with aggregation functionsL average (or avg), avg_zero,
// median, sum (or total), min, max, diff, stddev, count,
// range (or rangeOf), multiply & last (or current).
func aggregate(ctx *common.Context, series singlePathSpec, fname string) (ts.SeriesList, error) {
	switch fname {
	case emptyFnName, sumFnName, sumSeriesFnName, totalFnName:
		return sumSeries(ctx, multiplePathSpecs(series))
	case minFnName, minSeriesFnName:
		return minSeries(ctx, multiplePathSpecs(series))
	case maxFnName, maxSeriesFnName:
		return maxSeries(ctx, multiplePathSpecs(series))
	case medianFnName, medianSeriesFnName:
		return medianSeries(ctx, multiplePathSpecs(series))
	case avgFnName, averageFnName, averageSeriesFnName:
		return averageSeries(ctx, multiplePathSpecs(series))
	case multiplyFnName, multiplySeriesFnName:
		return multiplySeries(ctx, multiplePathSpecs(series))
	case diffFnName, diffSeriesFnName:
		return diffSeries(ctx, multiplePathSpecs(series))
	case countFnName, countSeriesFnName:
		return countSeries(ctx, multiplePathSpecs(series))
	case rangeFnName, rangeOfFnName, rangeOfSeriesFnName:
		return rangeOfSeries(ctx, series)
	case lastFnName, currentFnName:
		return lastSeries(ctx, multiplePathSpecs(series))
	case stddevFnName, stdevFnName, stddevSeriesFnName:
		return stddevSeries(ctx, multiplePathSpecs(series))
	default:
		// Median: the movingMedian() method already implemented is returning an series non compatible result. skip support for now.
		// avg_zero is not implemented, skip support for now unless later identified actual use cases.
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("invalid func %s", fname))
	}
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

// aggregateWithWildcards splits the given set of series into sub-groupings
// based on wildcard matches in the hierarchy, then aggregate the values in
// each grouping based on the given function.
func aggregateWithWildcards(
	ctx *common.Context,
	series singlePathSpec,
	fname string,
	positions ...int,
) (ts.SeriesList, error) {
	f, fexists := summarizeFuncs[fname]
	if !fexists {
		err := errors.NewInvalidParamsError(fmt.Errorf(
			"invalid func %s", fname))
		return ts.NewSeriesList(), err
	}

	return combineSeriesWithWildcards(ctx, series, positions, f.specificationFunc, f.consolidationFunc)
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
	for name, combinedSeries := range toCombine {
		seriesList := ts.SeriesList{
			Values:   combinedSeries,
			Metadata: series.Metadata,
		}
		combined, err := combineSeries(ctx, multiplePathSpecs(seriesList), name, f)
		if err != nil {
			return ts.NewSeriesList(), err
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

// splits a slice into chunks
func chunkArrayHelper(slice []string, numChunks int) [][]string {
	divided := make([][]string, 0, numChunks)

	chunkSize := (len(slice) + numChunks - 1) / numChunks

	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		if end > len(slice) {
			end = len(slice)
		}

		divided = append(divided, slice[i:end])
	}

	return divided
}

func evaluateTarget(ctx *common.Context, target string) (ts.SeriesList, error) {
	eng, ok := ctx.Engine.(*Engine)
	if !ok {
		return ts.NewSeriesList(), fmt.Errorf("engine not native engine")
	}
	expression, err := eng.Compile(target)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	return expression.Execute(ctx)
}

/*
applyByNode takes a seriesList and applies some complicated function (described by a string), replacing templates with unique
prefixes of keys from the seriesList (the key is all nodes up to the index given as `nodeNum`).

If the `newName` parameter is provided, the name of the resulting series will be given by that parameter, with any
"%" characters replaced by the unique prefix.

Example:

`applyByNode(servers.*.disk.bytes_free,1,"divideSeries(%.disk.bytes_free,sumSeries(%.disk.bytes_*))")`

Would find all series which match `servers.*.disk.bytes_free`, then trim them down to unique series up to the node
given by nodeNum, then fill them into the template function provided (replacing % by the prefixes).

Additional Examples:

Given keys of

- `stats.counts.haproxy.web.2XX`
- `stats.counts.haproxy.web.3XX`
- `stats.counts.haproxy.web.5XX`
- `stats.counts.haproxy.microservice.2XX`
- `stats.counts.haproxy.microservice.3XX`
- `stats.counts.haproxy.microservice.5XX`

The following will return the rate of 5XX's per service:

`applyByNode(stats.counts.haproxy.*.*XX, 3, "asPercent(%.5XX, sumSeries(%.*XX))", "%.pct_5XX")`

The output series would have keys `stats.counts.haproxy.web.pct_5XX` and `stats.counts.haproxy.microservice.pct_5XX`.
*/
func applyByNode(ctx *common.Context, seriesList singlePathSpec, nodeNum int, templateFunction string, newName string) (ts.SeriesList, error) {
	// using this as a set
	prefixMap := map[string]struct{}{}
	for _, series := range seriesList.Values {
		var (
			name = series.Name()

			partsSeen int
			prefix    string
		)

		for i, c := range name {
			if c == '.' {
				partsSeen++
				if partsSeen == nodeNum+1 {
					prefix = name[:i]
					break
				}
			}
		}

		if len(prefix) == 0 {
			continue
		}

		prefixMap[prefix] = struct{}{}
	}

	// transform to slice
	var prefixes []string
	for p := range prefixMap {
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		multiErr xerrors.MultiError

		output         = make([]*ts.Series, 0, len(prefixes))
		maxConcurrency = runtime.NumCPU() / 2
	)
	for _, prefixChunk := range chunkArrayHelper(prefixes, maxConcurrency) {
		if multiErr.LastError() != nil {
			return ts.NewSeriesList(), multiErr.LastError()
		}

		for _, prefix := range prefixChunk {
			newTarget := strings.ReplaceAll(templateFunction, "%", prefix)
			wg.Add(1)
			go func() {
				defer wg.Done()
				resultSeriesList, err := evaluateTarget(ctx, newTarget)

				if err != nil {
					mu.Lock()
					multiErr = multiErr.Add(err)
					mu.Unlock()
					return
				}

				mu.Lock()
				for _, resultSeries := range resultSeriesList.Values {
					if newName != "" {
						resultSeries = resultSeries.RenamedTo(strings.ReplaceAll(newName, "%", prefix))
					}
					resultSeries.Specification = prefix
					output = append(output, resultSeries)
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	r := ts.NewSeriesList()
	r.Values = output
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
	return groupByNodes(ctx, series, fname, []int{node}...)
}

func findFirstMetricExpression(seriesName string) (string, bool) {
	idxOfRightParen := strings.Index(seriesName, ")")
	if idxOfRightParen == -1 {
		return "", false
	}
	substring := seriesName[:idxOfRightParen]
	idxOfLeftParen := strings.LastIndex(substring, "(")
	if idxOfLeftParen == -1 {
		return "", false
	}
	return seriesName[idxOfLeftParen+1 : idxOfRightParen], true
}

func getParts(series *ts.Series) []string {
	seriesName := series.Name()
	if metricExpr, ok := findFirstMetricExpression(seriesName); ok {
		seriesName = metricExpr
	}
	return strings.Split(seriesName, ".")
}

func getAggregationKey(series *ts.Series, nodes []int) string {
	parts := getParts(series)

	keys := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n < 0 {
			n = len(parts) + n
		}

		if n < len(parts) {
			keys = append(keys, parts[n])
		} else {
			keys = append(keys, "")
		}
	}
	return strings.Join(keys, ".")
}

func getMetaSeriesGrouping(seriesList singlePathSpec, nodes []int) map[string][]*ts.Series {
	metaSeries := make(map[string][]*ts.Series)

	if len(nodes) > 0 {
		for _, s := range seriesList.Values {
			key := getAggregationKey(s, nodes)
			metaSeries[key] = append(metaSeries[key], s)
		}
	}

	return metaSeries
}

// Takes a serieslist and maps a callback to subgroups within as defined by multiple nodes
//
//      &target=groupByNodes(ganglia.server*.*.cpu.load*,"sum",1,4)
//
// Would return multiple series which are each the result of applying the “sum” aggregation to groups joined on the
// nodes’ list (0 indexed) resulting in a list of targets like
//
// 		sumSeries(ganglia.server1.*.cpu.load5),sumSeries(ganglia.server1.*.cpu.load10),sumSeries(ganglia.server1.*.cpu.load15),
// 		sumSeries(ganglia.server2.*.cpu.load5),sumSeries(ganglia.server2.*.cpu.load10),sumSeries(ganglia.server2.*.cpu.load15),...
//
// NOTE: if len(nodes) = 0, aggregate all series into 1 series.
func groupByNodes(ctx *common.Context, seriesList singlePathSpec, fname string, nodes ...int) (ts.SeriesList, error) {
	metaSeries := getMetaSeriesGrouping(seriesList, nodes)

	if len(metaSeries) == 0 {
		// if nodes is an empty slice or every node in nodes exceeds the number
		// of parts in each series, just treat it like aggregate
		return aggregate(ctx, seriesList, fname)
	}

	return applyFnToMetaSeries(ctx, seriesList, metaSeries, fname)
}

func applyFnToMetaSeries(ctx *common.Context, series singlePathSpec, metaSeries map[string][]*ts.Series, fname string) (ts.SeriesList, error) {
	if fname == "" {
		fname = sumFnName
	}

	f, fexists := summarizeFuncs[fname]
	if !fexists {
		return ts.NewSeriesList(), errors.NewInvalidParamsError(fmt.Errorf("invalid func %s", fname))
	}

	newSeries := make([]*ts.Series, 0, len(metaSeries))
	for key, metaSeries := range metaSeries {
		seriesList := ts.SeriesList{
			Values:   metaSeries,
			Metadata: series.Metadata,
		}
		output, err := combineSeries(ctx, multiplePathSpecs(seriesList), key, f.consolidationFunc)
		if err != nil {
			return ts.NewSeriesList(), err
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
		return ts.NewSeriesList(), err
	}

	consolidation := ts.NewConsolidation(ctx, start, end, millisPerStep, f)
	for _, s := range normalized.Values {
		consolidation.AddSeries(s, ts.Avg)
	}

	result := consolidation.BuildSeries(fname, ts.Finalize)
	return ts.SeriesList{
		Values:   []*ts.Series{result},
		Metadata: series.Metadata,
	}, nil
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
			return ts.NewSeriesList(), err
		}
	}

	for _, series := range weights.Values {
		if step != series.MillisPerStep() {
			err := errors.NewInvalidParamsError(fmt.Errorf("different step sizes in input series not supported"))
			return ts.NewSeriesList(), err
		}
	}

	valuesByKey, err := aliasByNode(ctx, input, node)
	if err != nil {
		return ts.NewSeriesList(), err
	}
	weightsByKey, err := aliasByNode(ctx, weights, node)
	if err != nil {
		return ts.NewSeriesList(), err
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

	meta := input.Metadata.CombineMetadata(weights.Metadata)
	top, err := sumSeries(ctx, multiplePathSpecs(ts.SeriesList{
		Values:   productSeries,
		Metadata: meta,
	}))
	if err != nil {
		return ts.NewSeriesList(), err
	}

	bottom, err := sumSeries(ctx, multiplePathSpecs(ts.SeriesList{
		Values:   consideredWeights,
		Metadata: meta,
	}))
	if err != nil {
		return ts.NewSeriesList(), err
	}

	results, err := divideSeries(ctx, singlePathSpec(top), singlePathSpec(bottom))
	if err != nil {
		return ts.NewSeriesList(), err
	}

	return alias(ctx, singlePathSpec(results), "weightedAverage")
}

// countSeries draws a horizontal line representing the number of nodes found in the seriesList.
func countSeries(ctx *common.Context, seriesList multiplePathSpecs) (ts.SeriesList, error) {
	count, err := common.Count(ctx, ts.SeriesList(seriesList), func(series ts.SeriesList) string {
		return wrapPathExpr(countSeriesFnName, series)
	})
	if err != nil {
		return ts.NewSeriesList(), err
	}

	r := ts.SeriesList(seriesList)
	r.Values = count.Values
	return r, nil
}
