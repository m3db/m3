package ts

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	xtest "github.com/m3db/m3/src/query/graphite/testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testSeries struct {
	name string
	data []float64
}

type testSortData struct {
	inputs []testSeries
	output []testSeries
}

func newTestSeriesValues(ctx context.Context, millisPerStep int, values []float64) Values {
	tsv := NewValues(ctx, millisPerStep, len(values))

	for i, n := range values {
		tsv.SetValueAt(i, n)
	}

	return tsv
}

func newTestSeriesList(ctx context.Context, start time.Time, inputs []testSeries, step int) []*Series {
	seriesList := make([]*Series, 0, len(inputs))

	for _, in := range inputs {
		series := NewSeries(ctx, in.name, start, newTestSeriesValues(ctx, step, in.data))
		seriesList = append(seriesList, series)
	}

	return seriesList
}

func validateOutputs(t *testing.T, step int, start time.Time, expected []testSeries, actual []*Series) {
	require.Equal(t, len(expected), len(actual))

	for i := range expected {
		a, e := actual[i], expected[i].data

		require.Equal(t, len(e), a.Len())

		for step := 0; step < a.Len(); step++ {
			v := a.ValueAt(step)
			xtest.Equalish(t, e[step], v, "invalid value for %d", step)
		}

		assert.Equal(t, expected[i].name, a.Name())
		assert.Equal(t, step, a.MillisPerStep())
		assert.Equal(t, start, a.StartTime())
	}
}

func testSortImpl(ctx context.Context, t *testing.T, tests []testSortData, sr SeriesReducer, dir Direction) {
	var (
		startTime = time.Now()
		step      = 100
	)

	for _, test := range tests {
		series := newTestSeriesList(ctx, startTime, test.inputs, step)

		output, err := SortSeries(series, sr, dir)

		require.NoError(t, err)
		validateOutputs(t, step, startTime, test.output, output)
	}
}

func TestSortSeries(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	testInput := []testSeries{
		{"foo", []float64{0, 601, 3, 4}},
		{"nan", []float64{math.NaN(), math.NaN(), math.NaN()}},
		{"bar", []float64{500, -8}},
		{"baz", []float64{600, -600, 3}},
		{"qux", []float64{100, 50000, 888, -1, -2}},
	}

	testSortImpl(ctx, t, []testSortData{
		{testInput, []testSeries{testInput[4], testInput[2], testInput[0], testInput[3], testInput[1]}},
	}, SeriesReducerAvg.Reducer(), Descending)

	testSortImpl(ctx, t, []testSortData{
		{testInput, []testSeries{testInput[0], testInput[3], testInput[4], testInput[2], testInput[1]}},
	}, SeriesReducerLast.Reducer(), Descending)

	testSortImpl(ctx, t, []testSortData{
		{testInput, []testSeries{testInput[4], testInput[0], testInput[3], testInput[2], testInput[1]}},
	}, SeriesReducerMax.Reducer(), Descending)

	testSortImpl(ctx, t, []testSortData{
		{testInput, []testSeries{testInput[4], testInput[3], testInput[2], testInput[0], testInput[1]}},
	}, SeriesReducerStdDev.Reducer(), Descending)

	testSortImpl(ctx, t, []testSortData{
		{testInput, []testSeries{testInput[1], testInput[3], testInput[0], testInput[2], testInput[4]}},
	}, SeriesReducerAvg.Reducer(), Ascending)

}
