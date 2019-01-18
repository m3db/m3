package querycontext

import (
	"fmt"
	"math"
	"sort"

	"github.com/m3db/m3/src/query/graphite/ts"
)

const (
	// FloatingPointFormat is the floating point format for naming
	FloatingPointFormat = "%.3f"
)

// ErrInvalidPercentile is used when the percentile specified is incorrect
func ErrInvalidPercentile(percentile float64) error {
	return fmt.Errorf("invalid percentile, percentile="+FloatingPointFormat, percentile)
}

// PercentileNamer formats a string with a percentile
type PercentileNamer func(name string, percentile float64) string

// ThresholdComparator compares two floats for other comparison
// functions such as Percentile checks.
type ThresholdComparator func(v, threshold float64) bool

// GreaterThan is a ThresholdComparator function for when
// a value is greater than a threshold
func GreaterThan(v, threshold float64) bool {
	return v > threshold
}

// LessThan is a ThresholdComparator function for when
// a value is less than a threshold
func LessThan(v, threshold float64) bool {
	return v < threshold
}

// SafeSort sorts the input slice and returns the number of NaNs in the input.
func SafeSort(input []float64) int {
	nans := 0
	for i := 0; i < len(input); i++ {
		if math.IsNaN(input[i]) {
			nans++
		}
	}

	sort.Float64s(input)
	return nans
}

// GetPercentile computes the percentile cut off for an array of floats
func GetPercentile(input []float64, percentile float64, interpolate bool) float64 {
	nans := SafeSort(input)
	series := input[nans:]
	if len(series) == 0 {
		return math.NaN()
	}

	fractionalRank := (percentile / 100.0) * (float64(len(series)))
	rank := math.Ceil(fractionalRank)

	rankAsInt := int(rank)

	if rankAsInt <= 1 {
		return series[0]
	}

	percentileResult := series[rankAsInt-1]

	if interpolate {
		prevValue := series[rankAsInt-2]
		fraction := fractionalRank - (rank - 1)
		percentileResult = prevValue + (fraction * (percentileResult - prevValue))
	}

	return percentileResult
}

// NPercentile returns percentile-percent of each series in the seriesList.
func NPercentile(ctx *Context, in ts.SeriesList, percentile float64, pn PercentileNamer) (ts.SeriesList, error) {
	if percentile < 0.0 || percentile > 100.0 {
		return ts.SeriesList{}, ErrInvalidPercentile(percentile)
	}
	results := make([]*ts.Series, 0, in.Len())
	for _, s := range in.Values {
		safeValues := s.SafeValues()
		if len(safeValues) == 0 {
			continue
		}
		percentileVal := GetPercentile(safeValues, percentile, false)
		if !math.IsNaN(percentileVal) {
			vals := ts.NewConstantValues(ctx, percentileVal, s.Len(), s.MillisPerStep())
			percentileSeries := ts.NewSeries(ctx, pn(s.Name(), percentile), s.StartTime(), vals)
			percentileSeries.Tags = s.Tags
			results = append(results, percentileSeries)
		}
	}
	in.Values = results
	return in, nil
}

// RemoveByPercentile removes all series above or below the given percentile, as
// determined by the PercentileComparator
func RemoveByPercentile(
	ctx *Context,
	in ts.SeriesList,
	percentile float64,
	pn PercentileNamer,
	tc ThresholdComparator,
) (ts.SeriesList, error) {
	results := make([]*ts.Series, 0, in.Len())
	for _, series := range in.Values {
		single := ts.SeriesList{Values: []*ts.Series{series}}
		percentileSeries, err := NPercentile(ctx, single, percentile, pn)
		if err != nil {
			return ts.SeriesList{}, err
		}

		numSteps := series.Len()
		vals := ts.NewValues(ctx, series.MillisPerStep(), numSteps)
		if percentileSeries.Len() == 1 {
			percentile := percentileSeries.Values[0].ValueAt(0)
			for i := 0; i < numSteps; i++ {
				v := series.ValueAt(i)
				if !tc(v, percentile) {
					vals.SetValueAt(i, v)
				}
			}
		}
		name := pn(series.Name(), percentile)
		newSeries := ts.NewSeries(ctx, name, series.StartTime(), vals)
		newSeries.Tags = series.Tags
		results = append(results, newSeries)
	}
	in.Values = results
	return in, nil
}
