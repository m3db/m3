package common

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	xtest "github.com/m3db/m3/src/query/graphite/testing"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSeries is used to create a tsdb.timeSeries
type TestSeries struct {
	Name string
	Data []float64
}

// TestSeriesWithTags is used to create a tsdb.timeSeries that contains tags
type TestSeriesWithTags struct {
	Series TestSeries
	Tags   map[string]string
}

// NewTestContext creates a new test context.
func NewTestContext() *Context {
	now := time.Now()
	return NewContext(ContextOptions{Start: now.Add(-time.Hour), End: now})
}

// NewTestSeriesValues creates a new ts.Values with given step size and values.
func NewTestSeriesValues(ctx context.Context, millisPerStep int, values []float64) ts.Values {
	tsv := ts.NewValues(ctx, millisPerStep, len(values))

	for i, n := range values {
		tsv.SetValueAt(i, n)
	}

	return tsv
}

// NewTestSeriesList creates a test series and values from a set of inputs
func NewTestSeriesList(ctx *Context, start time.Time, inputs []TestSeries, step int) []*ts.Series {
	seriesList := make([]*ts.Series, 0, len(inputs))

	for _, in := range inputs {
		series := ts.NewSeries(ctx, in.Name, start, NewTestSeriesValues(ctx, step, in.Data))
		seriesList = append(seriesList, series)
	}

	return seriesList
}

// NewConsolidationTestSeries returns multiple static series for consolidation
func NewConsolidationTestSeries(start, end time.Time, duration time.Duration) (*Context, []*ts.Series) {
	ctx := NewContext(ContextOptions{Start: start, End: end})

	testSeries := []*ts.Series{
		ts.NewSeries(ctx, "a", start,
			ts.NewConstantValues(ctx, 10, 6, 10000)),
		ts.NewSeries(ctx, "b", start.Add(-duration),
			ts.NewConstantValues(ctx, 15, 6, 10000)),
		ts.NewSeries(ctx, "c", start.Add(duration),
			ts.NewConstantValues(ctx, 17, 6, 10000)),
		ts.NewSeries(ctx, "d", start,
			ts.NewConstantValues(ctx, 3, 60, 1000)),
	}
	return ctx, testSeries
}

// CompareOutputsAndExpected compares the actual output with the expected output.
func CompareOutputsAndExpected(t *testing.T, step int, start time.Time, expected []TestSeries,
	actual []*ts.Series) {
	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		a := actual[i]
		require.Equal(t, expected[i].Name, a.Name())
		assert.Equal(t, step, a.MillisPerStep(), a.Name()+": MillisPerStep in expected series do not match MillisPerStep in actual")
		assert.Equal(t, start, a.StartTime(), a.Name()+": StartTime in expected series does not match StartTime in actual")
		e := expected[i].Data
		require.Equal(t, len(e), a.Len(), a.Name()+": length of expected series does not match length of actual")
		for step := 0; step < a.Len(); step++ {
			v := a.ValueAt(step)
			if math.IsNaN(e[step]) {
				assert.True(t, math.IsNaN(v), a.Name()+": invalid value for step %d/%d, should be NaN but is %v", step, a.Len(), v)
			} else {
				xtest.InDeltaWithNaNs(t, e[step], v, 0.0001, a.Name()+": invalid value for %d/%d", step, a.Len())
			}
		}
	}
}

// CompareOutputsAndExpectedWithTags compares two time series that contain tags
func CompareOutputsAndExpectedWithTags(t *testing.T, step int, start time.Time, expected []TestSeriesWithTags,
	actual []*ts.Series) {
	expectedSeries := make([]TestSeries, 0, len(expected))
	for _, series := range expected {
		expectedSeries = append(expectedSeries, series.Series)
	}
	CompareOutputsAndExpected(t, step, start, expectedSeries, actual)

	for i := range expected {
		assert.Equal(t, expected[i].Tags, actual[i].Tags)
	}
}

// MovingAverageStorage is a special test construct for the moving average function
type MovingAverageStorage struct {
	StepMillis     int
	Bootstrap      []float64
	Values         []float64
	BootstrapStart time.Time
}

// FetchByPath builds a new series from the input path
func (s *MovingAverageStorage) FetchByPath(
	ctx context.Context,
	path string,
	opts storage.FetchOptions,
) (*storage.FetchResult, error) {
	return s.fetchByIDs(ctx, []string{path}, opts)
}

// FetchByQuery builds a new series from the input query
func (s *MovingAverageStorage) FetchByQuery(
	ctx context.Context,
	query string,
	opts storage.FetchOptions,
) (*storage.FetchResult, error) {
	return s.fetchByIDs(ctx, []string{query}, opts)
}

// FetchByIDs builds a new series from the input query
func (s *MovingAverageStorage) fetchByIDs(
	ctx context.Context,
	ids []string,
	opts storage.FetchOptions,
) (*storage.FetchResult, error) {
	var seriesList []*ts.Series
	if s.Bootstrap != nil || s.Values != nil {
		var values []float64
		if opts.StartTime.Equal(s.BootstrapStart) {
			values = s.Bootstrap
		} else {
			values = s.Values
		}
		series := ts.NewSeries(ctx, ids[0], opts.StartTime, NewTestSeriesValues(ctx, s.StepMillis, values))
		seriesList = append(seriesList, series)
	}
	return storage.NewFetchResult(ctx, seriesList), nil
}

// Type returns to the dc scope
func (s *MovingAverageStorage) Type() storage.Type { return storage.TypeLocalDC }

// Name returns the name of the storage struct
func (s *MovingAverageStorage) Name() string { return "movingAverageStorage" }
