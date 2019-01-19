package native

import (
	"fmt"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/ts"
)

// fiveSigma uses a simple variance-based algorithm to identify values in the
// input timeseries that are unexpectedly high or low. The first 90% of each
// timeseries is used as a baseline for comparison. By definition, fiveSigma
// will consider >99.999% of a normally-distributed series unremarkable.
func fiveSigma(ctx *common.Context, input singlePathSpec) (ts.SeriesList, error) {
	if len(input.Values) < 1 {
		err := errors.NewInvalidParamsError(fmt.Errorf("fiveSigma can't run without at least one timeseries to check"))
		return ts.SeriesList{}, err
	}

	results := make([]*ts.Series, len(input.Values))
	for i, series := range input.Values {
		endOfBaseline := int(float64(series.Len()) * 0.9)
		baseline, err := series.Slice(0, endOfBaseline)
		if err != nil {
			return ts.SeriesList{}, err
		}
		s := baseline.CalcStatistics()
		labels := labelNSigma(ctx, series, s.Mean, s.StdDev, 5)
		results[i] = labels
	}

	r := ts.SeriesList(input)
	r.Values = results
	return r, nil
}

func labelNSigma(ctx *common.Context, series *ts.Series, avg, stdDev float64, n int) *ts.Series {
	var (
		min    = avg - (float64(n) * stdDev)
		max    = avg + (float64(n) * stdDev)
		values = ts.NewValues(ctx, series.MillisPerStep(), series.Len())
	)

	for step := 0; step < series.Len(); step++ {
		v := series.ValueAt(step)
		var outval float64
		if v < min {
			outval = -1
		} else if v > max {
			outval = 1
		} else {
			outval = 0
		}
		values.SetValueAt(step, outval)
	}

	return ts.NewSeries(ctx, fmt.Sprintf("%s.fiveSigmaAnomalies", series.Name()), series.StartTime(), values)
}
