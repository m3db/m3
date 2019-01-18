package querycontext

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRenamer(series *ts.Series) string {
	return fmt.Sprintf("test %v", series.Name())
}

func TestAbsolute(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	var (
		vals = []float64{-2, 0, 42, math.NaN()}
		step = 100
		now  = time.Now()
	)
	input := ts.SeriesList{
		Values: []*ts.Series{
			ts.NewSeries(ctx, "vodka", now, NewTestSeriesValues(ctx, step, vals)),
		},
	}

	r, err := Transform(ctx, input, NewStatelessTransformer(math.Abs), testRenamer)
	require.NoError(t, err)

	output := r.Values
	require.Equal(t, 1, len(output))

	abs := output[0]
	require.Equal(t, len(vals), abs.Len())
	assert.Equal(t, step, abs.MillisPerStep())
	assert.Equal(t, now, abs.StartTime())
	assert.Equal(t, "test vodka", abs.Name())

	absVals := make([]float64, len(vals))
	for i := 0; i < abs.Len(); i++ {
		absVals[i] = abs.ValueAt(i)
	}
	assert.Equal(t, []float64{2, 0, 42, math.NaN()}, absVals)
}

func TestOffset(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	var (
		tests = []struct {
			inputs []float64
			factor float64
			output []float64
		}{
			{
				[]float64{0, 1.0, 2.0, math.NaN(), 3.0}, 2.5,
				[]float64{2.5, 3.5, 4.5, math.NaN(), 5.5},
			},
			{
				[]float64{0, 1.0, 2.0, math.NaN(), 3.0}, -0.5,
				[]float64{-0.5, 0.5, 1.5, math.NaN(), 2.5},
			},
		}

		startTime = time.Now()
		step      = 100
	)

	for _, test := range tests {
		input := ts.SeriesList{
			Values: []*ts.Series{
				ts.NewSeries(ctx, "foo", startTime, NewTestSeriesValues(ctx, step, test.inputs)),
			},
		}

		r, err := Transform(ctx, input, NewStatelessTransformer(Offset(test.factor)), testRenamer)
		require.NoError(t, err)

		output := r.Values
		require.EqualValues(t, 1, len(output))
		require.Equal(t, len(test.inputs), output[0].Len())

		assert.Equal(t, step, output[0].MillisPerStep())
		assert.Equal(t, startTime, output[0].StartTime())
		assert.Equal(t, "test foo", output[0].Name())

		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			assert.Equal(t, test.output[step], v, "invalid value for %d", step)
		}
	}

}

func TestScale(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	var (
		tests = []struct {
			inputs []float64
			scale  float64
			output []float64
		}{
			{
				[]float64{0, 1.0, 2.0, math.NaN(), 3.0}, 2.5,
				[]float64{0, 2.5, 5.0, math.NaN(), 7.5},
			},
			{
				[]float64{0, 1.0, 2.0, math.NaN(), 3.0}, 0.5,
				[]float64{0, 0.5, 1.0, math.NaN(), 1.5},
			},
		}

		startTime = time.Now()
		step      = 100
	)

	for _, test := range tests {
		input := ts.SeriesList{
			Values: []*ts.Series{
				ts.NewSeries(ctx, "foo", startTime, NewTestSeriesValues(ctx, step, test.inputs)),
			},
		}

		r, err := Transform(ctx, input, NewStatelessTransformer(Scale(test.scale)), testRenamer)
		require.NoError(t, err)

		output := r.Values
		require.EqualValues(t, 1, len(output))
		require.Equal(t, len(test.inputs), output[0].Len())

		assert.EqualValues(t, step, output[0].MillisPerStep())
		assert.Equal(t, startTime, output[0].StartTime())
		assert.Equal(t, "test foo", output[0].Name())

		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			assert.Equal(t, test.output[step], v, "invalid value for %d", step)
		}
	}
}

func TestTransformNull(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	var (
		tests = []struct {
			inputs       []float64
			defaultValue float64
			output       []float64
		}{
			{
				[]float64{0, math.NaN(), 2.0, math.NaN(), 3.0}, 42.5,
				[]float64{0, 42.5, 2.0, 42.5, 3.0},
			},
			{
				[]float64{0, 1.0, 2.0, math.NaN(), 3.0}, -0.5,
				[]float64{0, 1.0, 2.0, -0.5, 3.0},
			},
		}

		startTime = time.Now()
		step      = 100
	)

	for _, test := range tests {
		input := ts.SeriesList{
			Values: []*ts.Series{
				ts.NewSeries(ctx, "foo", startTime, NewTestSeriesValues(ctx, step, test.inputs)),
			},
		}

		r, err := Transform(ctx, input, NewStatelessTransformer(TransformNull(test.defaultValue)), testRenamer)
		require.NoError(t, err)

		output := r.Values
		require.EqualValues(t, 1, len(output))
		require.Equal(t, len(test.inputs), output[0].Len())

		assert.EqualValues(t, step, output[0].MillisPerStep())
		assert.Equal(t, startTime, output[0].StartTime())
		assert.Equal(t, "test foo", output[0].Name())

		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			assert.Equal(t, test.output[step], v, "invalid value for %d", step)
		}
	}
}

func TestIsNonNull(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	var (
		tests = []struct {
			inputs []float64
			output []float64
		}{
			{
				[]float64{0, math.NaN(), 2.0, math.NaN(), 3.0},
				[]float64{1, 0, 1, 0, 1},
			},
			{
				[]float64{0, 1.0, 2.0, math.NaN(), 3.0},
				[]float64{1, 1, 1, 0, 1},
			},
		}

		startTime = time.Now()
		step      = 100
	)

	for _, test := range tests {
		input := ts.SeriesList{
			Values: []*ts.Series{
				ts.NewSeries(ctx, "foo", startTime, NewTestSeriesValues(ctx, step, test.inputs)),
			},
		}

		r, err := Transform(ctx, input, NewStatelessTransformer(IsNonNull()), testRenamer)
		require.NoError(t, err)

		output := r.Values
		require.EqualValues(t, 1, len(output))
		require.Equal(t, len(test.inputs), output[0].Len())

		assert.EqualValues(t, step, output[0].MillisPerStep())
		assert.Equal(t, startTime, output[0].StartTime())
		assert.Equal(t, "test foo", output[0].Name())

		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			assert.Equal(t, test.output[step], v, "invalid value for %d", step)
		}
	}
}

func TestStdev(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	startTime := ctx.StartTime
	stepSize := 10000
	inputs := []struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		{
			"foo",
			startTime,
			stepSize,
			[]float64{1.0, 2.0, 3.0, 4.0, nan, nan, nan, 5.0, 6.0, nan, nan},
		},
	}

	inputSeries := make([]*ts.Series, 0, len(inputs))
	for _, input := range inputs {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		inputSeries = append(inputSeries, series)
	}
	expected := []TestSeries{
		TestSeries{Name: "foo | stddev 3", Data: []float64{0.0, 0.5, 0.8165, 0.8165, 0.5, 0.0, nan, 0.0, 0.5, 0.5, 0.0}},
	}
	input := ts.SeriesList{Values: inputSeries}
	results, err := Stdev(ctx, input, 3, 0.1, func(series *ts.Series, points int) string {
		return fmt.Sprintf("%s | stddev %d", series.Name(), points)
	})
	require.Nil(t, err)
	CompareOutputsAndExpected(t, stepSize, startTime, expected, results.Values)
}

func TestPerSecond(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	startTime := ctx.StartTime
	stepSize := 1000
	inputs := []struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		{
			"foo",
			startTime,
			stepSize,
			[]float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
		},
		{
			"foo",
			startTime,
			stepSize,
			[]float64{1.0, 2.0, 4.0, 7.0, 11.0, 16.0, 22.0, 29.0, 37.0, 46.0},
		},
		{
			"foo",
			startTime,
			stepSize,
			[]float64{1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0},
		},
		{
			"foo",
			startTime,
			stepSize,
			[]float64{nan, nan, nan, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0},
		},
		{
			"foo",
			startTime,
			stepSize,
			[]float64{1.0, 2.0, 3.0, nan, nan, nan, 7.0, 8.0, 9.0, 10.0},
		},
	}

	inputSeries := make([]*ts.Series, 0, len(inputs))
	for _, input := range inputs {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		inputSeries = append(inputSeries, series)
	}
	expected := []TestSeries{
		TestSeries{Name: "foo | perSecond", Data: []float64{nan, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}},
		TestSeries{Name: "foo | perSecond", Data: []float64{nan, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}},
		TestSeries{Name: "foo | perSecond", Data: []float64{nan, 1.0, 1.0, 1.0, 1.0, nan, 1.0, 1.0, 1.0, 1.0}},
		TestSeries{Name: "foo | perSecond", Data: []float64{nan, nan, nan, nan, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}},
		TestSeries{Name: "foo | perSecond", Data: []float64{nan, 1.0, 1.0, nan, nan, nan, 1.0, 1.0, 1.0, 1.0}},
	}
	input := ts.SeriesList{Values: inputSeries}
	results, err := PerSecond(ctx, input, func(series *ts.Series) string {
		return fmt.Sprintf("%s | perSecond", series.Name())
	})
	require.Nil(t, err)
	CompareOutputsAndExpected(t, stepSize, startTime, expected, results.Values)
}
