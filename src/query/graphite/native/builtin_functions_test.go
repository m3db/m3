package native

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// testInput defines the input for various tests
	testInput = []common.TestSeries{
		{"foo", []float64{0, 601, 3, 4}},
		{"nan", []float64{math.NaN(), math.NaN(), math.NaN()}},
		{"bar", []float64{500, -8}},
		{"baz", []float64{600, -600, 3}},
		{"quux", []float64{100, 50000, 888, -1, -2, math.NaN()}},
	}

	// testSmallInput defines a small input for various tests
	testSmallInput = []common.TestSeries{
		testInput[0],
		testInput[2],
	}

	// testInputWithNaNSeries defines another input set with all-nan series
	testInputWithNaNSeries = []common.TestSeries{
		testInput[0],
		testInput[2],
		testInput[4],
		{"allNaN", []float64{math.NaN(), math.NaN()}},
	}
)

func TestExclude(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)

	g01 := ts.NewSeries(ctx, "servers.graphite01-sjc1.disk.internal-root.available_bytes", now, values)
	g02 := ts.NewSeries(ctx, "servers.graphite02-sjc1.disk.internal-root.available_bytes", now, values)
	g03 := ts.NewSeries(ctx, "servers.graphite03-sjc1.disk.internal-root.available_bytes", now, values)

	sampleInput := []*ts.Series{g01, g02, g03}
	sampleOutput := []*ts.Series{g01, g03}
	tests := []struct {
		inputs  []*ts.Series
		r       string
		n       int
		outputs []*ts.Series
	}{
		{
			sampleInput,
			"graphite02-sjc1",
			2,
			sampleOutput,
		},
		{
			sampleInput,
			"graphite",
			0,
			[]*ts.Series{},
		},
		{
			sampleInput,
			"graphite.*-sjc1",
			0,
			[]*ts.Series{},
		},
	}

	for _, test := range tests {
		results, err := exclude(nil, singlePathSpec{
			Values: test.inputs,
		}, test.r)
		require.Nil(t, err)
		require.NotNil(t, results)
		require.Equal(t, test.n, results.Len())
		for i := range results.Values {
			assert.Equal(t, sampleOutput[i].Name(), results.Values[i].Name())
		}
	}
}

func TestExcludeErr(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)

	series := []*ts.Series{
		ts.NewSeries(ctx, "anything", now, values),
	}
	results, err := exclude(ctx, singlePathSpec{
		Values: series,
	}, "(")
	require.Error(t, err, "Failure is expected")
	require.Nil(t, results.Values)
}

func TestSortByName(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	values := ts.NewConstantValues(ctx, 10.0, 1000, 10)

	series := []*ts.Series{
		ts.NewSeries(ctx, "b.d.a", now, values),
		ts.NewSeries(ctx, "zee", now, values),
		ts.NewSeries(ctx, "a.c.d", now, values),
	}

	results, err := sortByName(ctx, singlePathSpec{
		Values: series,
	})
	require.Nil(t, err)
	require.Equal(t, len(series), results.Len())
	assert.Equal(t, "a.c.d", results.Values[0].Name())
	assert.Equal(t, "b.d.a", results.Values[1].Name())
	assert.Equal(t, "zee", results.Values[2].Name())
}

func getTestInput(ctx *common.Context) []*ts.Series {
	series := make([]*ts.Series, len(testInput))
	now := time.Now()
	for idx, s := range testInput {
		series[idx] = ts.NewSeries(ctx, s.Name, now, common.NewTestSeriesValues(ctx, 100, s.Data))
	}
	return series
}

func testSortingFuncs(
	t *testing.T,
	f func(ctx *common.Context, series singlePathSpec) (ts.SeriesList, error),
	resultIndexes []int,
) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	input := getTestInput(ctx)
	results, err := f(ctx, singlePathSpec{Values: input})
	require.Nil(t, err)
	require.Equal(t, len(resultIndexes), results.Len())
	for i, idx := range resultIndexes {
		require.Equal(t, results.Values[i], input[idx])
	}
}

func TestSortByTotal(t *testing.T) {
	testSortingFuncs(t, sortByTotal, []int{4, 0, 2, 3, 1})
}

func TestSortByMaxima(t *testing.T) {
	testSortingFuncs(t, sortByMaxima, []int{4, 0, 3, 2, 1})
}

func TestAbsolute(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	inputVals := []float64{-2, 0, 42, math.NaN()}
	outputVals := []float64{2, 0, 42, math.NaN()}
	start := time.Now()

	input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 100, inputVals))
	r, err := absolute(ctx, singlePathSpec{
		Values: []*ts.Series{input},
	})
	require.NoError(t, err)

	outputs := r.Values
	require.Equal(t, 1, len(outputs))
	require.Equal(t, 100, outputs[0].MillisPerStep())
	require.Equal(t, len(outputVals), outputs[0].Len())
	require.Equal(t, start, outputs[0].StartTime())
	assert.Equal(t, "absolute(foo)", outputs[0].Name())

	for step := 0; step < outputs[0].Len(); step++ {
		v := outputs[0].ValueAt(step)
		assert.Equal(t, outputVals[step], v, "invalid value for %d", step)
	}
}

func TestScale(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		inputs  []float64
		scale   float64
		outputs []float64
	}{
		{
			[]float64{0, 1.0, 2.0, math.NaN(), 3.0},
			2.5,
			[]float64{0, 2.5, 5.0, math.NaN(), 7.5},
		},
		{
			[]float64{0, 1.0, 2.0, math.NaN(), 3.0},
			0.5,
			[]float64{0, 0.5, 1.0, math.NaN(), 1.5},
		},
	}

	start := time.Now()
	for _, test := range tests {
		input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 100, test.inputs))
		r, err := scale(ctx, singlePathSpec{
			Values: []*ts.Series{input},
		}, test.scale)
		require.NoError(t, err)

		outputs := r.Values
		require.Equal(t, 1, len(outputs))
		require.Equal(t, 100, outputs[0].MillisPerStep())
		require.Equal(t, len(test.inputs), outputs[0].Len())
		require.Equal(t, start, outputs[0].StartTime())
		assert.Equal(t, fmt.Sprintf("scale(foo,"+common.FloatingPointFormat+")", test.scale), outputs[0].Name())

		for step := 0; step < outputs[0].Len(); step++ {
			v := outputs[0].ValueAt(step)
			assert.Equal(t, test.outputs[step], v, "invalid value for %d", step)
		}
	}
}

func TestPercentileOfSeriesErrors(t *testing.T) {
	ctx := common.NewTestContext()

	tests := []struct {
		stepPerMillis         []int
		percentile            float64
		values                [][]float64
		expectedValues        []float64
		expectedStepPerMillis float64
		interpolate           genericInterface
	}{
		{ // percentile is over 100%.
			[]int{120, 120},
			101.0,
			[][]float64{
				{60.0, 50.0, 40.0, 30.0, 20.0, 10.0},
				{6, 5, 4, 3, 2, 1},
			},
			[]float64{5.5, 11.0, 16.5, 22.0, 27.5, 33.0},
			120,
			"true",
		},
		{ // percentile is less than zero.
			[]int{120, 120},
			-10.0,
			[][]float64{
				{60.0, 50.0, 40.0, 30.0, 20.0, 10.0},
				{6, 5, 4, 3, 2, 1},
			},
			[]float64{5.5, 11.0, 16.5, 22.0, 27.5, 33.0},
			120,
			"true",
		},
		{ // percentile input is empty.
			[]int{120, 120},
			10.0,
			[][]float64{},
			[]float64{},
			120,
			"true",
		},
		{ // percentile series have different size millisPerStep.
			[]int{120, 320},
			33.0,
			[][]float64{
				{60.0, 50.0, 40.0, 30.0, 20.0, 10.0},
				{6, 5, 4, 3, 2, 1},
			},
			[]float64{5.5, 11.0, 16.5, 22.0, 27.5, 33.0},
			960,
			"true",
		},
		{ // interpolateStr is neither "true" nor "false".
			[]int{120, 320},
			33.0,
			[][]float64{
				{60.0, 50.0, 40.0, 30.0, 20.0, 10.0},
				{6, 5, 4, 3, 2, 1},
			},
			[]float64{5.5, 11.0, 16.5, 22.0, 27.5, 33.0},
			960,
			"random",
		},
		{ // types other than boolean and string are not allowed
			[]int{120, 120, 120, 120, 120},
			33.0,
			[][]float64{
				{math.NaN(), 16, 23, math.NaN(), 75, 48, 42, 41},
				{math.NaN(), 36, 74, 43, 73},
				{math.NaN(), 61, 24, 29, math.NaN(), 62, 65, 72},
				{math.NaN(), 48, 94, math.NaN(), 32, 39, math.NaN(), 84},
				{math.NaN(), 16, math.NaN(), 85, 34, 27, 74, math.NaN(), 72},
			},
			[]float64{math.NaN(), 16, 24, 43, 34},
			120,
			[]*ts.Series(nil),
		},
	}

	for _, test := range tests {
		seriesList := make([]*ts.Series, len(test.values))
		for i := 0; i < len(seriesList); i++ {
			seriesList[i] = ts.NewSeries(ctx, "<values>", time.Now(), common.NewTestSeriesValues(ctx, test.stepPerMillis[i], test.values[i]))
		}

		_, err := percentileOfSeries(ctx, singlePathSpec{
			Values: seriesList,
		}, test.percentile, test.interpolate)
		assert.NotNil(t, err)
	}
}

func TestPercentileOfSeries(t *testing.T) {
	ctx := common.NewTestContext()

	tests := []struct {
		stepPerMillis         []int
		percentile            float64
		values                [][]float64
		expectedValues        []float64
		expectedStepPerMillis float64
		interpolate           genericInterface
	}{
		{ // Test arrays with NaNs, multiple series, and same time step.
			[]int{120, 120, 120, 120, 120},
			33,
			[][]float64{
				{math.NaN(), 16, 23, math.NaN(), 75, 48, 42, 41},
				{math.NaN(), 36, 74, 43, 73},
				{math.NaN(), 61, 24, 29, math.NaN(), 62, 65, 72},
				{math.NaN(), 48, 94, math.NaN(), 32, 39, math.NaN(), 84},
				{math.NaN(), 16, math.NaN(), 85, 34, 27, 74, math.NaN(), 72},
			},
			[]float64{math.NaN(), 16, 24, 29, 34},
			120,
			"false",
		},
		{ // Test arrays with NaNs, multiple series, and same time step.
			[]int{120, 120, 120, 120, 120},
			33,
			[][]float64{
				{math.NaN(), 16, 23, math.NaN(), 75, 48, 42, 41},
				{math.NaN(), 36, 74, 43, 73},
				{math.NaN(), 61, 24, 29, math.NaN(), 62, 65, 72},
				{math.NaN(), 48, 94, math.NaN(), 32, 39, math.NaN(), 84},
				{math.NaN(), 16, math.NaN(), 85, 34, 27, 74, math.NaN(), 72},
			},
			[]float64{math.NaN(), 16.0, 23.32, 29, 32.64},
			120,
			"true",
		},
		{ // Test arrays with NaNs remove them and get correct percentile value
			[]int{120, 120, 120},
			5,
			[][]float64{
				{math.NaN(), 60, 50, 40, math.NaN(), 30, 20, 10},
				{math.NaN(), 15, 12, 9, 6, 3, math.NaN()},
				{math.NaN(), 6, 5, 4, 3, 2, 1},
			},
			[]float64{math.NaN(), 6, 5, 4, 3, 2, 1},
			120,
			"false",
		},
		{ // Test non-interpolated percentile
			[]int{120, 120},
			42,
			[][]float64{
				{60, 5, 40, 30, 20, 10},
				{3, 40, 4, 1, 2, 6},
			},
			[]float64{3, 5, 4, 1, 2, 6},
			120,
			"false",
		},
		{ // Test non-interpolated percentile for 100th percentile
			[]int{120, 120, 120},
			100,
			[][]float64{
				{60, 50, 40, 30, 20, 10},
				{18, 15, 12, 9, 6, 3},
				{6, 5, 4, 3, 2, 1},
			},
			[]float64{60, 50, 40, 30, 20, 10},
			120,
			"false",
		},
		{ // Test non-interpolated percentile for 1st percentile
			[]int{120, 120},
			1,
			[][]float64{
				{60, 50, 40, 30, 20, 10},
				{6, 5, 4, 3, 2, 1},
			},
			[]float64{6, 5, 4, 3, 2, 1},
			120,
			"false",
		},
		{ // Test interpolation for a percentile series
			[]int{120, 120},
			75,
			[][]float64{
				{60, 50, 40, 30, 20, 10},
				{6, 5, 4, 3, 2, 1},
			},
			[]float64{33, 27.5, 22, 16.5, 11, 5.5},
			120,
			"true",
		},
	}

	for _, test := range tests {
		seriesList := make([]*ts.Series, len(test.values))
		for i := 0; i < len(seriesList); i++ {
			seriesList[i] = ts.NewSeries(ctx, "<values>", time.Now(), common.NewTestSeriesValues(ctx, test.stepPerMillis[i], test.values[i]))
		}

		r, err := percentileOfSeries(ctx, singlePathSpec{
			Values: seriesList,
		}, test.percentile, test.interpolate)
		require.NoError(t, err)

		output := r.Values
		name := fmt.Sprintf("percentileOfSeries(<values>,"+common.FloatingPointFormat+")",
			test.percentile)
		assert.Equal(t, name, output[0].Name())
		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			require.NoError(t, err)

			assert.Equal(t, test.expectedValues[step], v)
		}
		assert.Equal(t, test.expectedStepPerMillis, output[0].MillisPerStep())
	}
}

func TestOffset(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		inputs  []float64
		factor  float64
		outputs []float64
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

	start := time.Now()
	for _, test := range tests {
		input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 100, test.inputs))
		r, err := offset(ctx, singlePathSpec{
			Values: []*ts.Series{input},
		}, test.factor)
		require.NoError(t, err)

		outputs := r.Values
		require.Equal(t, 1, len(outputs))
		require.Equal(t, 100, outputs[0].MillisPerStep())
		require.Equal(t, len(test.inputs), outputs[0].Len())
		require.Equal(t, start, outputs[0].StartTime())
		assert.Equal(t, fmt.Sprintf("offset(foo,"+common.FloatingPointFormat+")", test.factor), outputs[0].Name())

		for step := 0; step < outputs[0].Len(); step++ {
			v := outputs[0].ValueAt(step)
			assert.Equal(t, test.outputs[step], v, "invalid value for %d", step)
		}
	}

}

func TestPerSecond(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		millisPerStep int
		input         []float64
		output        []float64
	}{
		// increase by 1 per 100ms == 10 per sec
		{100, []float64{1, 2, 3, 4, 5}, []float64{math.NaN(), 10, 10, 10, 10}},

		// increase by 1 per 10s == .1 per sec
		{10000, []float64{1, 2, 3, 4, 5}, []float64{math.NaN(), 0.1, 0.1, 0.1, 0.1}},

		// decreasing value - rate of change not applicable
		{1000, []float64{5, 4, 3, 2, 1},
			[]float64{math.NaN(), math.NaN(), math.NaN(), math.NaN(), math.NaN()}},

		// skip over missing values
		{1000, []float64{1, 2, math.NaN(), 4, 5}, []float64{math.NaN(), 1, math.NaN(), 1, 1}},
	}

	for _, test := range tests {
		values := common.NewTestSeriesValues(ctx, test.millisPerStep, test.input)
		series := ts.NewSeries(ctx, "foo", time.Now(), values)
		r, err := perSecond(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		}, math.NaN())
		require.NoError(t, err)

		perSec := r.Values
		require.Equal(t, 1, len(perSec))
		require.Equal(t, len(test.output), perSec[0].Len())
		assert.Equal(t, series.StartTime(), perSec[0].StartTime())
		assert.Equal(t, "perSecond(foo)", perSec[0].Name())
		for i := 0; i < perSec[0].Len(); i++ {
			val := perSec[0].ValueAt(i)
			assert.Equal(t, test.output[i], val, "invalid value for %d", i)
		}
	}
}

func TestTransformNull(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		inputs       []float64
		defaultValue float64
		outputs      []float64
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

	start := time.Now()
	for _, test := range tests {
		input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 100, test.inputs))
		r, err := transformNull(ctx, singlePathSpec{
			Values: []*ts.Series{input},
		}, test.defaultValue)
		require.NoError(t, err)

		outputs := r.Values
		require.Equal(t, 1, len(outputs))
		require.Equal(t, 100, outputs[0].MillisPerStep())
		require.Equal(t, len(test.inputs), outputs[0].Len())
		require.Equal(t, start, outputs[0].StartTime())
		assert.Equal(t, fmt.Sprintf("transformNull(foo,"+common.FloatingPointFormat+")", test.defaultValue), outputs[0].Name())

		for step := 0; step < outputs[0].Len(); step++ {
			v := outputs[0].ValueAt(step)
			assert.Equal(t, test.outputs[step], v, "invalid value for %d", step)
		}
	}
}

var (
	testMovingAverageBootstrap = testMovingAverageStart.Add(-30 * time.Second)
	testMovingAverageStart     = time.Now().Truncate(time.Minute)
	testMovingAverageEnd       = testMovingAverageStart.Add(time.Minute)
)

func testMovingAverage(t *testing.T, target, expectedName string, values, bootstrap, output []float64) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	engine := NewEngine(
		&common.MovingAverageStorage{
			StepMillis:     10000,
			Bootstrap:      bootstrap,
			BootstrapStart: testMovingAverageBootstrap,
			Values:         values,
		},
	)
	phonyContext := common.NewContext(common.ContextOptions{
		Start:  testMovingAverageStart,
		End:    testMovingAverageEnd,
		Engine: engine,
	})

	expr, err := phonyContext.Engine.(*Engine).Compile(target)
	require.NoError(t, err)
	res, err := expr.Execute(phonyContext)
	require.NoError(t, err)
	var expected []common.TestSeries
	if output != nil {
		expectedSeries := common.TestSeries{
			Name: expectedName,
			Data: output,
		}
		expected = append(expected, expectedSeries)
	}
	common.CompareOutputsAndExpected(t, 10000, testMovingAverageStart,
		expected, res.Values)
}

func TestMovingAverageSuccess(t *testing.T) {
	values := []float64{12.0, 19.0, -10.0, math.NaN(), 10.0}
	bootstrap := []float64{3.0, 4.0, 5.0}
	expected := []float64{4.0, 7.0, 12.0, 7.0, 4.5}
	testMovingAverage(t, "movingAverage(foo.bar.baz, '30s')", "movingAverage(foo.bar.baz,\"30s\")", values, bootstrap, expected)
	testMovingAverage(t, "movingAverage(foo.bar.baz, 3)", "movingAverage(foo.bar.baz,3)", values, bootstrap, expected)
	testMovingAverage(t, "movingAverage(foo.bar.baz, 3)", "movingAverage(foo.bar.baz,3)", nil, nil, nil)

	bootstrapEntireSeries := []float64{3.0, 4.0, 5.0, 12.0, 19.0, -10.0, math.NaN(), 10.0}
	testMovingAverage(t, "movingAverage(foo.bar.baz, '30s')", "movingAverage(foo.bar.baz,\"30s\")", values, bootstrapEntireSeries, expected)
	testMovingAverage(t, "movingAverage(foo.bar.baz, 3)", "movingAverage(foo.bar.baz,3)", values, bootstrapEntireSeries, expected)
}

func testMovingAverageError(t *testing.T, target string) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	engine := NewEngine(
		&common.MovingAverageStorage{
			StepMillis:     10000,
			Bootstrap:      []float64{1.0},
			BootstrapStart: testMovingAverageBootstrap,
			Values:         []float64{1.0},
		},
	)
	phonyContext := common.NewContext(common.ContextOptions{
		Start:  testMovingAverageStart,
		End:    testMovingAverageEnd,
		Engine: engine,
	})

	expr, err := phonyContext.Engine.(*Engine).Compile(target)
	require.NoError(t, err)
	res, err := expr.Execute(phonyContext)
	require.Error(t, err)
	require.Nil(t, res.Values)
}

func TestMovingAverageError(t *testing.T) {
	testMovingAverageError(t, "movingAverage(foo.bar.baz, '-30s')")
	testMovingAverageError(t, "movingAverage(foo.bar.baz, 0)")
}

func TestIsNonNull(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		inputs  []float64
		outputs []float64
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

	start := time.Now()
	for _, test := range tests {
		input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 100, test.inputs))
		r, err := isNonNull(ctx, singlePathSpec{
			Values: []*ts.Series{input},
		})
		require.NoError(t, err)

		outputs := r.Values
		require.Equal(t, 1, len(outputs))
		require.Equal(t, 100, outputs[0].MillisPerStep())
		require.Equal(t, len(test.inputs), outputs[0].Len())
		require.Equal(t, start, outputs[0].StartTime())
		assert.Equal(t, "isNonNull(foo)", outputs[0].Name())

		for step := 0; step < outputs[0].Len(); step++ {
			v := outputs[0].ValueAt(step)
			assert.Equal(t, test.outputs[step], v, "invalid value for %d", step)
		}
	}
}

func TestKeepLastValue(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		inputs  []float64
		outputs []float64
		limit   int
	}{
		{
			[]float64{0, math.NaN(), 2.0, math.NaN(), 3.0},
			[]float64{0, 0, 2.0, 2.0, 3.0},
			-1,
		},
		{
			[]float64{math.NaN(), 1.0, 2.0, math.NaN(), 3.0},
			[]float64{math.NaN(), 1.0, 2.0, 2.0, 3.0},
			-1,
		},
		{
			[]float64{1.0, math.NaN(), math.NaN(), math.NaN(), 3.0, math.NaN(), math.NaN(), 2.0},
			[]float64{1.0, math.NaN(), math.NaN(), math.NaN(), 3.0, 3.0, 3.0, 2.0},
			2,
		},
	}

	start := time.Now()
	for _, test := range tests {
		input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 100, test.inputs))
		outputs, err := keepLastValue(ctx, singlePathSpec{
			Values: []*ts.Series{input},
		}, test.limit)
		expected := common.TestSeries{Name: "keepLastValue(foo)", Data: test.outputs}
		require.NoError(t, err)
		common.CompareOutputsAndExpected(t, 100, start,
			[]common.TestSeries{expected}, outputs.Values)
	}
}

func TestSustainedAbove(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		inputs    []float64
		outputs   []float64
		threshold float64
		interval  string
	}{
		{
			[]float64{0, 0, 3, 3, 4, 0, 0},
			[]float64{0, 0, 3, 3, 4, 0, 0},
			2,
			"10s",
		},
		{
			[]float64{0, 0, 3, 3, 4, 0, 0},
			[]float64{0, 0, 0, 3, 4, 0, 0},
			2,
			"20s",
		},
		{
			[]float64{0, 0, 3, 3, 4, 0, 0},
			[]float64{0, 0, 0, 0, 4, 0, 0},
			2,
			"30s",
		},
		{
			[]float64{0, 0, 3, 3, 4, 0, 0},
			[]float64{0, 0, 0, 0, 0, 0, 0},
			2,
			"40s",
		},
		{
			[]float64{0, 3, 3, 4, 4, 2, 0},
			[]float64{0, 0, 0, 0, 4, 0, 0},
			4,
			"20s",
		},
		{
			[]float64{1, 2, 3, 4, 9, 9, 9, 9, 9, 3},
			[]float64{0, 0, 0, 0, 0, 0, 9, 9, 9, 0},
			8,
			"30s",
		},
		{
			[]float64{1, 2, 3, 4, 5, 5, 5, 5, 5, 3},
			[]float64{0, 0, 0, 4, 5, 5, 5, 5, 5, 0},
			4,
			"10s",
		},
		{
			[]float64{-3, -4, -1, 3, 0, -1, -5, -6, -3},
			[]float64{-4, -4, -4, 3, 0, -1, -4, -4, -4},
			-2,
			"20s",
		},
	}

	start := time.Now()
	for _, test := range tests {
		input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 10000, test.inputs))
		r, err := sustainedAbove(ctx, singlePathSpec{
			Values: []*ts.Series{input},
		}, test.threshold, test.interval)
		require.NoError(t, err)

		outputs := r.Values
		require.Equal(t, 1, len(outputs))
		require.Equal(t, 10000, outputs[0].MillisPerStep())
		require.Equal(t, len(test.inputs), outputs[0].Len())
		require.Equal(t, start, outputs[0].StartTime())

		str := fmt.Sprintf("sustainedAbove(foo, %f, '%s')", test.threshold, test.interval)

		assert.Equal(t, str, outputs[0].Name())

		for step := 0; step < outputs[0].Len(); step++ {
			v := outputs[0].ValueAt(step)

			assert.Equal(t, test.outputs[step], v, "invalid value for %d", step)
		}
	}
}

func TestSustainedAboveFail(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	input := ts.NewSeries(ctx, "foo", time.Now(), common.NewTestSeriesValues(ctx, 10000, []float64{0}))
	outputs, err := sustainedAbove(ctx, singlePathSpec{
		Values: []*ts.Series{input},
	}, 10, "wat")
	require.Error(t, err)
	require.Equal(t, 0, outputs.Len())
}

func TestSustainedBelow(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		inputs    []float64
		outputs   []float64
		threshold float64
		interval  string
	}{
		{
			[]float64{4, 4, 1, 1, 1, 4, 4},
			[]float64{4, 4, 1, 1, 1, 4, 4},
			2,
			"10s",
		},
		{
			[]float64{7, 8, 3, 3, 2, 6, 7},
			[]float64{6, 6, 6, 3, 2, 6, 6},
			3,
			"20s",
		},
		{
			[]float64{9, 7, 3, 3, 2, 5, 6},
			[]float64{6, 6, 6, 6, 2, 6, 6},
			3,
			"30s",
		},
		{
			[]float64{8, 5, 3, 3, 2, 5, 8},
			[]float64{6, 6, 6, 6, 6, 6, 6},
			3,
			"40s",
		},
		{
			[]float64{4, 3, 3, 1, 1, 2, 4},
			[]float64{2, 2, 2, 2, 1, 2, 2},
			1,
			"20s",
		},
		{
			[]float64{7, 8, 9, 2, 2, 4, 2, 5, 3, 2},
			[]float64{8, 8, 8, 8, 8, 8, 2, 8, 8, 8},
			4,
			"40s",
		},
		{
			[]float64{1, 2, 3, 4, 9, 9, 9, 9, 9, 3},
			[]float64{8, 2, 3, 4, 8, 8, 8, 8, 8, 8},
			4,
			"20s",
		},
		{
			[]float64{-3, -4, -3, -1, 3, 2, -5, -4, -3, -3},
			[]float64{0, -4, -3, 0, 0, 0, 0, -4, -3, -3},
			-2,
			"20s",
		},
	}

	start := time.Now()
	for _, test := range tests {
		input := ts.NewSeries(ctx, "foo", start, common.NewTestSeriesValues(ctx, 10000, test.inputs))
		r, err := sustainedBelow(ctx, singlePathSpec{
			Values: []*ts.Series{input},
		}, test.threshold, test.interval)
		require.NoError(t, err)

		outputs := r.Values
		require.Equal(t, 1, len(outputs))
		require.Equal(t, 10000, outputs[0].MillisPerStep())
		require.Equal(t, len(test.inputs), outputs[0].Len())
		require.Equal(t, start, outputs[0].StartTime())

		str := fmt.Sprintf("sustainedBelow(foo, %f, '%s')", test.threshold, test.interval)

		assert.Equal(t, str, outputs[0].Name())
		for step := 0; step < outputs[0].Len(); step++ {
			v := outputs[0].ValueAt(step)

			assert.Equal(t, test.outputs[step], v, "invalid value for %d", step)
		}
	}
}

func TestSustainedBelowFail(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	input := ts.NewSeries(ctx, "foo", time.Now(), common.NewTestSeriesValues(ctx, 10000, []float64{0}))
	outputs, err := sustainedBelow(ctx, singlePathSpec{
		Values: []*ts.Series{input},
	}, 10, "wat")
	require.Error(t, err)
	require.Equal(t, 0, outputs.Len())
}

// testSeries is used to create a tsdb.timeSeries
type testSeries struct {
	name string
	data []float64
}

// nIntParamGoldenData holds test data for functions that take an additional "n" int parameter
type nIntParamGoldenData struct {
	inputs  []common.TestSeries
	n       int
	outputs []common.TestSeries
}

// rankingFunc selects the n lowest or highest series based on certain metric of the
// series (e.g., maximum, minimum, average).
type rankingFunc func(ctx *common.Context, input singlePathSpec, n int) (ts.SeriesList, error)

func testRanking(t *testing.T, ctx *common.Context, tests []nIntParamGoldenData, f rankingFunc) {
	start := time.Now()
	step := 100
	for _, test := range tests {
		outputs, err := f(ctx, singlePathSpec{
			Values: generateSeriesList(ctx, start, test.inputs, step),
		}, test.n)
		if test.n < 0 {
			require.NotNil(t, err)
			require.Equal(t, "n must be positive", err.Error())
			assert.Nil(t, outputs.Values, "Nil timeseries should be returned")
			continue
		}
		require.NoError(t, err)
		common.CompareOutputsAndExpected(t, step, start,
			test.outputs, outputs.Values)
	}
}

func TestHighestCurrent(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []nIntParamGoldenData{
		{
			testInput,
			0,
			nil,
		},
		{
			testInput,
			1,
			[]common.TestSeries{testInput[0]},
		},
		{
			testInput,
			2,
			[]common.TestSeries{testInput[0], testInput[3]},
		},
		{
			testInput,
			len(testInput) + 10, // force sort
			[]common.TestSeries{testInput[0], testInput[3], testInput[4], testInput[2], testInput[1]},
		},
	}
	testRanking(t, ctx, tests, highestCurrent)
}

func TestHighestCurrentWithNaNSeries(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []nIntParamGoldenData{
		{
			testInputWithNaNSeries,
			0,
			nil,
		},
		{
			testInputWithNaNSeries,
			1,
			[]common.TestSeries{testInputWithNaNSeries[0]},
		},
		{
			testInputWithNaNSeries,
			2,
			[]common.TestSeries{testInputWithNaNSeries[0], testInputWithNaNSeries[2]},
		},
		{
			testInputWithNaNSeries,
			3,
			[]common.TestSeries{testInputWithNaNSeries[0], testInputWithNaNSeries[2], testInputWithNaNSeries[1]},
		},
		{
			testInputWithNaNSeries,
			4,
			[]common.TestSeries{testInputWithNaNSeries[0], testInputWithNaNSeries[2], testInputWithNaNSeries[1], testInputWithNaNSeries[3]},
		},
	}
	testRanking(t, ctx, tests, highestCurrent)
}

func TestHighestAverage(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []nIntParamGoldenData{
		{
			testInput,
			1,
			[]common.TestSeries{testInput[4]},
		},
		{
			testInput,
			2,
			[]common.TestSeries{testInput[4], testInput[2]},
		},
	}
	testRanking(t, ctx, tests, highestAverage)
}

func TestHighestMax(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []nIntParamGoldenData{
		{
			testInput,
			1,
			[]common.TestSeries{testInput[4]},
		},
		{
			testInput,
			2,
			[]common.TestSeries{testInput[4], testInput[0]},
		},
	}
	testRanking(t, ctx, tests, highestMax)
}

func TestFallbackSeries(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		input    []common.TestSeries
		fallback []common.TestSeries
		output   []common.TestSeries
	}{
		{
			nil,
			[]common.TestSeries{common.TestSeries{"output", []float64{0, 1.0}}},
			[]common.TestSeries{common.TestSeries{"output", []float64{0, 1.0}}},
		},
		{
			[]common.TestSeries{},
			[]common.TestSeries{common.TestSeries{"output", []float64{0, 1.0}}},
			[]common.TestSeries{common.TestSeries{"output", []float64{0, 1.0}}},
		},
		{
			[]common.TestSeries{common.TestSeries{"output", []float64{0, 2.0}}},
			[]common.TestSeries{common.TestSeries{"fallback", []float64{0, 1.0}}},
			[]common.TestSeries{common.TestSeries{"output", []float64{0, 2.0}}},
		},
	}

	start := time.Now()
	step := 100
	for _, test := range tests {

		inputs := generateSeriesList(ctx, start, test.input, step)
		fallbacks := generateSeriesList(ctx, start, test.fallback, step)

		outputs, err := fallbackSeries(ctx, singlePathSpec{
			Values: inputs,
		}, singlePathSpec{
			Values: fallbacks,
		})
		require.NoError(t, err)

		common.CompareOutputsAndExpected(t, step, start,
			test.output, outputs.Values)
	}
}

func TestMostDeviant(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []nIntParamGoldenData{
		{
			testInput,
			-2,
			nil,
		},
		{
			testInput,
			1,
			[]common.TestSeries{testInput[4]},
		},
		{
			testInput,
			2,
			[]common.TestSeries{testInput[4], testInput[3]},
		},
	}
	testRanking(t, ctx, tests, mostDeviant)
}

func TestLowestAverage(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []nIntParamGoldenData{
		{
			testInput,
			0,
			nil,
		},
		{
			testInput,
			1,
			[]common.TestSeries{testInput[1]},
		},
		{
			testInput,
			2,
			[]common.TestSeries{testInput[1], testInput[3]},
		},
		{
			testInput,
			3,
			[]common.TestSeries{testInput[1], testInput[3], testInput[0]},
		},
	}
	testRanking(t, ctx, tests, lowestAverage)
}

func TestLowestCurrent(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []nIntParamGoldenData{
		{
			testInput,
			0,
			nil,
		},
		{
			testInput,
			1,
			[]common.TestSeries{testInput[1]},
		},
		{
			testInput,
			2,
			[]common.TestSeries{testInput[1], testInput[2]},
		},
		{
			testInput,
			3,
			[]common.TestSeries{testInput[1], testInput[2], testInput[4]},
		},
	}
	testRanking(t, ctx, tests, lowestCurrent)
}

type comparatorFunc func(ctx *common.Context, series singlePathSpec, n float64) (ts.SeriesList, error)

func testComparatorFunc(
	t *testing.T,
	f comparatorFunc,
	n float64,
	resultIndexes []int,
) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	input := getTestInput(ctx)
	results, err := f(ctx, singlePathSpec{
		Values: input,
	}, n)
	require.Nil(t, err)
	require.Equal(t, len(resultIndexes), results.Len())
	for i, idx := range resultIndexes {
		require.Equal(t, input[idx], results.Values[i])
	}
}

func TestMaximumAbove(t *testing.T) {
	testComparatorFunc(t, maximumAbove, -10, []int{0, 2, 3, 4})
	testComparatorFunc(t, maximumAbove, 600, []int{0, 4})
	testComparatorFunc(t, maximumAbove, 100000, nil)
}

func TestMinimumAbove(t *testing.T) {
	testComparatorFunc(t, minimumAbove, -1000, []int{0, 2, 3, 4})
	testComparatorFunc(t, minimumAbove, -100, []int{0, 2, 4})
	testComparatorFunc(t, minimumAbove, 1, nil)
}

func TestAverageAbove(t *testing.T) {
	testComparatorFunc(t, averageAbove, 0, []int{0, 2, 3, 4})
	testComparatorFunc(t, averageAbove, 1, []int{0, 2, 4})
	testComparatorFunc(t, averageAbove, 12000, nil)
}

func TestCurrentAbove(t *testing.T) {
	testComparatorFunc(t, currentAbove, -10, []int{0, 2, 3, 4})
	testComparatorFunc(t, currentAbove, -5, []int{0, 3, 4})
	testComparatorFunc(t, currentAbove, 5, nil)
}

func TestCurrentBelow(t *testing.T) {
	testComparatorFunc(t, currentBelow, 5, []int{0, 2, 3, 4})
	testComparatorFunc(t, currentBelow, 0, []int{2, 4})
	testComparatorFunc(t, currentBelow, -5, []int{2})
	testComparatorFunc(t, currentBelow, -10, nil)
}

func TestRemoveBelowValue(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	tests := []struct {
		inputs  []common.TestSeries
		n       float64
		outputs []common.TestSeries
	}{
		{
			testSmallInput,
			500,
			[]common.TestSeries{
				{"foo", []float64{nan, 601, nan, nan}},
				{"bar", []float64{500, nan}},
			},
		},
		{
			testSmallInput,
			4,
			[]common.TestSeries{
				{"foo", []float64{nan, 601, nan, 4}},
				{"bar", []float64{500, nan}},
			},
		},
	}
	start := time.Now()
	step := 100
	for _, test := range tests {
		outputs, err := removeBelowValue(ctx, singlePathSpec{
			Values: generateSeriesList(ctx, start, test.inputs, step),
		}, test.n)
		require.NoError(t, err)
		for i := range test.outputs { // overwrite series names
			name := fmt.Sprintf("removeBelowValue(%s, "+common.FloatingPointFormat+")",
				test.outputs[i].Name, test.n)
			test.outputs[i].Name = name
		}
		common.CompareOutputsAndExpected(t, step, start,
			test.outputs, outputs.Values)
	}
}

func TestRemoveAboveValue(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	tests := []struct {
		inputs  []common.TestSeries
		n       float64
		outputs []common.TestSeries
	}{
		{
			testSmallInput,
			500,
			[]common.TestSeries{
				{"foo", []float64{0, nan, 3, 4}},
				{"bar", []float64{500, -8}},
			},
		},
		{
			testSmallInput,
			3,
			[]common.TestSeries{
				{"foo", []float64{0, nan, 3, nan}},
				{"bar", []float64{nan, -8}},
			},
		},
	}
	start := time.Now()
	step := 100
	for _, test := range tests {
		outputs, err := removeAboveValue(ctx, singlePathSpec{
			Values: generateSeriesList(ctx, start, test.inputs, step),
		}, test.n)
		require.NoError(t, err)
		for i := range test.outputs { // overwrite series names
			test.outputs[i].Name = fmt.Sprintf(
				"removeAboveValue(%s, "+common.FloatingPointFormat+")",
				test.outputs[i].Name,
				test.n,
			)
		}
		common.CompareOutputsAndExpected(t, step, start,
			test.outputs, outputs.Values)
	}
}

func TestRemoveEmptySeries(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	tests := []struct {
		inputs  []common.TestSeries
		outputs []common.TestSeries
	}{
		{
			[]common.TestSeries{
				{"foo", []float64{nan, 601, nan, nan}},
				{"bar", []float64{500, nan}},
				{"baz", []float64{nan, nan, nan}},
			},
			[]common.TestSeries{
				{"foo", []float64{nan, 601, nan, nan}},
				{"bar", []float64{500, nan}},
			},
		},
	}
	start := time.Now()
	step := 100
	for _, test := range tests {
		outputs, err := removeEmptySeries(ctx, singlePathSpec{
			Values: generateSeriesList(ctx, start, test.inputs, step),
		})
		require.NoError(t, err)
		common.CompareOutputsAndExpected(t, step, start,
			test.outputs, outputs.Values)
	}
}

func generateSeriesList(ctx *common.Context, start time.Time, inputs []common.TestSeries, step int) []*ts.Series {
	tSeriesList := make([]*ts.Series, 0, len(inputs))
	for _, in := range inputs {
		tSeries := ts.NewSeries(ctx, in.Name, start, common.NewTestSeriesValues(ctx, step, in.Data))
		tSeriesList = append(tSeriesList, tSeries)
	}
	return tSeriesList
}

func TestScaleToSeconds(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		millisPerStep int
		values        []float64
		expected      []float64
		seconds       int
	}{
		{
			1000,
			[]float64{1000.0, 2000.0, 3000.0, 4000.0, 5000.0},
			[]float64{2000.0, 4000.0, 6000.0, 8000.0, 10000.0},
			2,
		},
		// expected values should double when step is halved
		// relative to the original expected values
		{
			500,
			[]float64{1000.0, 2000.0, 3000.0, 4000.0, 5000.0},
			[]float64{4000.0, 8000.0, 12000.0, 16000.0, 20000.0},
			2,
		},
		// expected values should drop by a factor of 1/5 when step is multiplied by 5
		// relative to the original expected values
		{
			5000,
			[]float64{1000.0, 2000.0, 3000.0, 4000.0, 5000.0},
			[]float64{400.0, 800.0, 1200.0, 1600.0, 2000.0},
			2,
		},
	}

	for _, test := range tests {
		timeSeries := ts.NewSeries(ctx, "<values>", ctx.StartTime,
			common.NewTestSeriesValues(ctx, test.millisPerStep, test.values))

		r, err := scaleToSeconds(ctx, singlePathSpec{
			Values: []*ts.Series{timeSeries},
		}, test.seconds)
		require.NoError(t, err)

		output := r.Values
		require.Equal(t, 1, len(output))
		assert.Equal(t, "scaleToSeconds(<values>,2)", output[0].Name())
		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			assert.Equal(t, test.expected[step], v)
		}
	}
}

func TestAsPercentWithSeriesTotal(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		valuesStep int
		values     []float64
		totalsStep int
		totals     []float64
		outputStep int
		output     []float64
	}{
		{
			100, []float64{10.0, 20.0, 30.0, 40.0, 50.0},
			100, []float64{1000.0, 1000.0, 1000.0, 1000.0, 1000.0},
			100, []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		},
		{
			100, []float64{12.0, 14.0, 16.0, math.NaN(), 20.0},
			150, []float64{50.0, 50.0, 25.0, 50.0, 50.0},
			300, []float64{28.0, 53.0},
		},
	}

	for _, test := range tests {
		timeSeries := ts.NewSeries(ctx, "<values>", ctx.StartTime,
			common.NewTestSeriesValues(ctx, test.valuesStep, test.values))
		totalSeries := ts.NewSeries(ctx, "<totals>", ctx.StartTime,
			common.NewTestSeriesValues(ctx, test.totalsStep, test.totals))

		r, err := asPercent(ctx, singlePathSpec{
			Values: []*ts.Series{timeSeries},
		}, ts.SeriesList{
			Values: []*ts.Series{totalSeries},
		})
		require.NoError(t, err, fmt.Sprintf("err: %v", err))

		output := r.Values
		require.Equal(t, 1, len(output))
		require.Equal(t, output[0].MillisPerStep(), test.outputStep)
		assert.Equal(t, "asPercent(<values>, <totals>)", output[0].Name())

		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			assert.Equal(t, math.Trunc(v), test.output[step])
		}
	}
}

func TestAsPercentWithFloatTotal(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	tests := []struct {
		valuesStep int
		values     []float64
		total      float64
		outputStep int
		output     []float64
	}{
		{
			100, []float64{12.0, 14.0, 16.0, nan, 20.0},
			20.0,
			100, []float64{60, 70, 80, nan, 100},
		},
		{
			100, []float64{12.0, 14.0, 16.0, nan, 20.0},
			0,
			100, []float64{nan, nan, nan, nan, nan},
		},
	}

	for _, test := range tests {
		timeSeries := ts.NewSeries(ctx, "<values>", ctx.StartTime,
			common.NewTestSeriesValues(ctx, test.valuesStep, test.values))
		r, err := asPercent(ctx, singlePathSpec{
			Values: []*ts.Series{timeSeries},
		}, test.total)
		require.NoError(t, err)

		output := r.Values
		require.Equal(t, 1, len(output))
		require.Equal(t, output[0].MillisPerStep(), test.outputStep)
		expectedName := fmt.Sprintf("asPercent(<values>, "+common.FloatingPointFormat+")",
			test.total)
		assert.Equal(t, expectedName, output[0].Name())

		for step := 0; step < output[0].Len(); step++ {
			v := output[0].ValueAt(step)
			assert.Equal(t, math.Trunc(v), test.output[step])
		}
	}
}

func TestAsPercentWithSeriesList(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	inputs := []struct {
		name   string
		step   int
		values []float64
	}{
		{
			"foo",
			100,
			[]float64{12.0, 14.0, 16.0, nan, 20.0, 30.0},
		},
		{
			"bar",
			200,
			[]float64{7.0, nan, 25.0},
		},
	}
	outputs := []struct {
		name   string
		step   int
		values []float64
	}{
		{
			"asPercent(foo, foo)",
			200,
			[]float64{65.0, 100.0, 50.0},
		},
		{
			"asPercent(bar, bar)",
			200,
			[]float64{35.0, nan, 50.0},
		},
	}

	var inputSeries []*ts.Series
	for _, input := range inputs {
		timeSeries := ts.NewSeries(
			ctx,
			input.name,
			ctx.StartTime,
			common.NewTestSeriesValues(ctx, input.step, input.values),
		)
		inputSeries = append(inputSeries, timeSeries)
	}

	var expected []*ts.Series
	for _, output := range outputs {
		timeSeries := ts.NewSeries(
			ctx,
			output.name,
			ctx.StartTime,
			common.NewTestSeriesValues(ctx, output.step, output.values),
		)
		expected = append(expected, timeSeries)
	}

	for _, totalArg := range []interface{}{
		ts.SeriesList{Values: []*ts.Series(nil)},
		singlePathSpec{},
	} {
		r, err := asPercent(ctx, singlePathSpec{
			Values: inputSeries,
		}, totalArg)
		require.NoError(t, err)

		results := r.Values
		require.Equal(t, len(expected), len(results))
		for i := 0; i < len(results); i++ {
			require.Equal(t, expected[i].MillisPerStep(), results[i].MillisPerStep())
			require.Equal(t, expected[i].Len(), results[i].Len())
			require.Equal(t, expected[i].Name(), results[i].Name())
			for step := 0; step < results[i].Len(); step++ {
				require.Equal(t, expected[i].ValueAt(step), results[i].ValueAt(step))
			}
		}
	}

}

func testLogarithm(t *testing.T, base int, indices []int) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	invals := make([]float64, 101)
	for i := range invals {
		invals[i] = float64(i)
	}

	series := ts.NewSeries(ctx, "hello", time.Now(),
		common.NewTestSeriesValues(ctx, 10000, invals))

	r, err := logarithm(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, base)
	require.NoError(t, err)

	output := r.Values
	require.Equal(t, 1, len(output))
	assert.Equal(t, fmt.Sprintf("log(hello, %d)", base), output[0].Name())
	assert.Equal(t, series.StartTime(), output[0].StartTime())
	require.Equal(t, len(invals), output[0].Len())
	assert.Equal(t, math.NaN(), output[0].ValueAt(0))
	assert.Equal(t, 0, output[0].ValueAt(indices[0]))
	assert.Equal(t, 1, output[0].ValueAt(indices[1]))
	assert.Equal(t, 2, output[0].ValueAt(indices[2]))
}

func TestLogarithm(t *testing.T) {
	testLogarithm(t, 10, []int{1, 10, 100})
	testLogarithm(t, 2, []int{1, 2, 4})

	_, err := logarithm(nil, singlePathSpec{}, -1)
	require.NotNil(t, err)
}

func TestIntegral(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	invals := []float64{
		0, 1, 2, 3, 4, 5, 6, math.NaN(), 8, math.NaN(),
	}

	outvals := []float64{
		0, 1, 3, 6, 10, 15, 21, math.NaN(), 29, math.NaN(),
	}

	series := ts.NewSeries(ctx, "hello", time.Now(),
		common.NewTestSeriesValues(ctx, 10000, invals))

	r, err := integral(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	})
	require.NoError(t, err)

	output := r.Values
	require.Equal(t, 1, len(output))
	assert.Equal(t, "integral(hello)", output[0].Name())
	assert.Equal(t, series.StartTime(), output[0].StartTime())
	require.Equal(t, len(outvals), output[0].Len())
	for i, expected := range outvals {
		assert.Equal(t, expected, output[0].ValueAt(i), "incorrect value at %d", i)
	}
}

func TestDerivative(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		values []float64
		output []float64
	}{
		{
			[]float64{10.0, 20.0, 30.0, 5.0, 5.0},
			[]float64{math.NaN(), 10.0, 10.0, -25.0, 0.0},
		},
		{
			[]float64{50.0, 50.0, 25.0, 250.0, 350.0},
			[]float64{math.NaN(), 0.0, -25.0, 225.0, 100.0},
		},
	}

	start := time.Now()
	step := 100
	for _, test := range tests {
		input := []common.TestSeries{{"foo", test.values}}
		expected := []common.TestSeries{{"derivative(foo)", test.output}}
		timeSeries := generateSeriesList(ctx, start, input, step)
		output, err := derivative(ctx, singlePathSpec{
			Values: timeSeries,
		})
		require.NoError(t, err)
		common.CompareOutputsAndExpected(t, step, start,
			expected, output.Values)
	}
}

func TestNonNegativeDerivative(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	tests := []struct {
		values   []float64
		maxValue float64
		output   []float64
	}{
		{
			[]float64{10.0, 20.0, 30.0, 5.0, 5.0},
			math.NaN(),
			[]float64{math.NaN(), 10.0, 10.0, math.NaN(), 0.0},
		},
		{
			[]float64{50.0, 50.0, 25.0, 250.0, 350.0},
			100.0,
			[]float64{math.NaN(), 0.0, 76.0, 225.0, 100.0},
		},
	}

	start := time.Now()
	step := 100
	for _, test := range tests {
		input := []common.TestSeries{{"foo", test.values}}
		expected := []common.TestSeries{{"nonNegativeDerivative(foo)", test.output}}
		timeSeries := generateSeriesList(ctx, start, input, step)
		output, err := nonNegativeDerivative(ctx, singlePathSpec{
			Values: timeSeries,
		}, test.maxValue)
		require.NoError(t, err)
		common.CompareOutputsAndExpected(t, step, start, expected, output.Values)
	}
}

type TimeSeriesPtrVector []*ts.Series

func (o TimeSeriesPtrVector) Len() int           { return len(o) }
func (o TimeSeriesPtrVector) Less(i, j int) bool { return o[i].Name() < o[j].Name() }
func (o TimeSeriesPtrVector) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

func TestConstantLine(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	testValue := 5.0
	r, err := constantLine(ctx, testValue)
	require.Nil(t, err)

	testSeries := r.Values
	require.Equal(t, 1, len(testSeries))
	require.Equal(t, 2, testSeries[0].Len())
	expectedName := fmt.Sprintf(common.FloatingPointFormat, testValue)
	require.Equal(t, expectedName, testSeries[0].Name())
	for i := 0; i < testSeries[0].Len(); i++ {
		require.Equal(t, float64(testValue), testSeries[0].ValueAt(i))
	}
}

func TestIdentity(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	testName := "testName.mytest"
	r, err := identity(ctx, testName)
	require.Nil(t, err)

	testSeries := r.Values
	require.Equal(t, 1, len(testSeries))
	require.Equal(t, testName, testSeries[0].Name())
	require.Equal(t, 60, testSeries[0].Len())
	expectedValue := ctx.StartTime.Unix()
	for i := 0; i < testSeries[0].Len(); i++ {
		require.Equal(t, float64(expectedValue), testSeries[0].ValueAt(i))
		expectedValue += 60
	}
}

func TestLimit(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	// invalid input
	testInput := getTestInput(ctx)
	testSeries, err := limit(ctx, singlePathSpec{
		Values: testInput,
	}, -1)
	require.NotNil(t, err)

	// valid input
	testSeries, err = limit(ctx, singlePathSpec{
		Values: testInput,
	}, 1)
	require.Nil(t, err)
	require.Equal(t, 1, testSeries.Len())

	// input bigger than length of series
	testSeries, err = limit(ctx, singlePathSpec{
		Values: testInput,
	}, 10)
	require.Nil(t, err)
	require.Equal(t, len(testInput), testSeries.Len())
}

func TestHitCount(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := time.Now()
	tests := []struct {
		name           string
		startTime      time.Time
		stepInMilli    int
		values         []float64
		intervalString string
		newStartTime   time.Time
		newStep        int
		output         []float64
	}{
		{
			"foo",
			now,
			1000,
			[]float64{1.0, 2.0, 3.0, 4.0, 5.0, math.NaN(), 6.0},
			"2s",
			now.Add(-time.Second),
			2000,
			[]float64{1.0, 5.0, 9.0, 6.0},
		},
		{
			"bar",
			now,
			1000,
			[]float64{1.0, 2.0, 3.0, 4.0, 5.0, math.NaN(), 6.0},
			"10s",
			now.Add(-3 * time.Second),
			10000,
			[]float64{21.0},
		},
	}

	for _, input := range tests {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		results, err := hitcount(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		}, input.intervalString)
		expected := common.TestSeries{
			Name: fmt.Sprintf(`hitcount(%s, %q)`, input.name, input.intervalString),
			Data: input.output,
		}
		require.Nil(t, err)
		common.CompareOutputsAndExpected(t, input.newStep, input.newStartTime,
			[]common.TestSeries{expected}, results.Values)
	}
}

func TestSubstr(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	now := ctx.StartTime
	input := struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		"aliasByName(foo.bar,baz)",
		now,
		1000,
		[]float64{1.0, 2.0, 3.0},
	}

	series := ts.NewSeries(
		ctx,
		input.name,
		input.startTime,
		common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
	)
	results, err := substr(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, 1, 0)
	expected := common.TestSeries{Name: "bar", Data: input.values}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, input.stepInMilli, input.startTime,
		[]common.TestSeries{expected}, results.Values)

	results, err = substr(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, 0, 2)
	expected = common.TestSeries{Name: "foo.bar", Data: input.values}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, input.stepInMilli, input.startTime,
		[]common.TestSeries{expected}, results.Values)

	results, err = substr(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, 0, 0)
	expected = common.TestSeries{Name: "foo.bar", Data: input.values}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, input.stepInMilli, input.startTime,
		[]common.TestSeries{expected}, results.Values)

	results, err = substr(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, 2, 1)
	require.NotNil(t, err)

	results, err = substr(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, -1, 1)
	require.NotNil(t, err)

	results, err = substr(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, 3, 4)
	require.NotNil(t, err)
}

type mockStorage struct{}

func (*mockStorage) FetchByQuery(
	ctx xctx.Context, query string, opts storage.FetchOptions,
) (*storage.FetchResult, error) {
	if query == "add warning" {
		ctx.AddWarning(fmt.Errorf("warning"))
	}
	return storage.NewFetchResult(ctx, nil), nil
}

func (*mockStorage) Name() string { return "mock" }

func (*mockStorage) Type() storage.Type { return storage.TypeLocalDC }

func TestHoltWintersForecast(t *testing.T) {
	ctx := common.NewTestContext()
	ctx.Engine = NewEngine(
		&mockStorage{},
	)
	defer ctx.Close()

	now := ctx.StartTime
	tests := []struct {
		name         string
		startTime    time.Time
		stepInMilli  int
		values       []float64
		duration     time.Duration
		newStartTime time.Time
		newStep      int
		output       []float64
	}{
		{
			"foo",
			now,
			1000,
			[]float64{4.0, 5.0, 6.0},
			3 * time.Second,
			now,
			1000,
			[]float64{4.0, 4.0, 4.10035},
		},
	}

	for _, input := range tests {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		results, err := holtWintersForecastInternal(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		}, input.duration)
		expected := common.TestSeries{
			Name: fmt.Sprintf(`holtWintersForecast(%s)`, input.name),
			Data: input.output,
		}
		require.Nil(t, err)
		common.CompareOutputsAndExpected(t, input.newStep, input.newStartTime,
			[]common.TestSeries{expected}, results.Values)
	}
}

func TestHoltWintersConfidenceBands(t *testing.T) {
	ctx := common.NewTestContext()
	ctx.Engine = NewEngine(
		&mockStorage{},
	)
	defer ctx.Close()

	now := ctx.StartTime
	tests := []struct {
		name           string
		startTime      time.Time
		stepInMilli    int
		values         []float64
		duration       time.Duration
		lowerStartTime time.Time
		lowerStep      int
		lowerOutput    []float64
		upperStartTime time.Time
		upperStep      int
		upperOutput    []float64
	}{
		{
			"foo",
			now,
			1000,
			[]float64{4.0, 5.0, 6.0},
			3 * time.Second,
			now,
			1000,
			[]float64{0.4787, 3.7, 3.5305},
			now,
			1000,
			[]float64{2.1039, 4.3, 4.6702},
		},
	}

	for _, input := range tests {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		results, err := holtWintersConfidenceBandsInternal(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		}, 3, input.duration)
		lowerExpected := common.TestSeries{
			Name: fmt.Sprintf(`holtWintersConfidenceLower(%s)`, input.name),
			Data: input.lowerOutput,
		}
		upperExpected := common.TestSeries{
			Name: fmt.Sprintf(`holtWintersConfidenceUpper(%s)`, input.name),
			Data: input.upperOutput,
		}
		require.Nil(t, err)
		common.CompareOutputsAndExpected(t, input.lowerStep, input.lowerStartTime,
			[]common.TestSeries{lowerExpected}, []*ts.Series{results.Values[0]})
		common.CompareOutputsAndExpected(t, input.upperStep, input.upperStartTime,
			[]common.TestSeries{upperExpected}, []*ts.Series{results.Values[1]})
	}
}

func TestHoltWintersAberration(t *testing.T) {
	ctx := common.NewTestContext()
	ctx.Engine = NewEngine(
		&mockStorage{},
	)
	defer ctx.Close()

	now := ctx.StartTime
	tests := []struct {
		name                string
		startTime           time.Time
		stepInMilli         int
		values              []float64
		duration            time.Duration
		aberrationStartTime time.Time
		aberrationStep      int
		aberrationOutput    []float64
	}{
		{
			"foo",
			now,
			1000,
			[]float64{4.0, 5.0, 6.0},
			3 * time.Second,
			now,
			1000,
			[]float64{0, 0.7, 1.3298},
		},
	}

	for _, input := range tests {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		results, err := holtWintersAberrationInternal(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		}, 3, input.duration)
		expected := common.TestSeries{
			Name: fmt.Sprintf(`holtWintersAberration(%s)`, input.name),
			Data: input.aberrationOutput,
		}
		require.Nil(t, err)
		common.CompareOutputsAndExpected(t, input.aberrationStep, input.aberrationStartTime,
			[]common.TestSeries{expected}, results.Values)
	}
}

func TestSquareRoot(t *testing.T) {
	ctx := common.NewTestContext()
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
			[]float64{1.0, -2.0, 3.0, nan},
		},
		{
			"bar",
			startTime,
			stepSize,
			[]float64{4.0},
		},
	}

	inputSeries := make([]*ts.Series, 0, len(inputs))
	for _, input := range inputs {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		inputSeries = append(inputSeries, series)
	}
	expected := []common.TestSeries{
		common.TestSeries{Name: "squareRoot(foo)", Data: []float64{1.0, nan, 1.73205, nan}},
		common.TestSeries{Name: "squareRoot(bar)", Data: []float64{2.0}},
	}
	results, err := squareRoot(ctx, singlePathSpec{
		Values: inputSeries,
	})
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, stepSize, startTime,
		expected, results.Values)
}

func TestStdev(t *testing.T) {
	ctx := common.NewTestContext()
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
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		inputSeries = append(inputSeries, series)
	}
	expected := []common.TestSeries{
		common.TestSeries{Name: "stddev(foo,3)", Data: []float64{0.0, 0.5, 0.8165, 0.8165, 0.5, 0.0, nan, 0.0, 0.5, 0.5, 0.0}},
	}
	results, err := stdev(ctx, singlePathSpec{
		Values: inputSeries,
	}, 3, 0.1)
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, stepSize, startTime,
		expected, results.Values)
}

func TestRangeOfSeries(t *testing.T) {
	ctx, input := newConsolidationTestSeries()
	defer ctx.Close()

	expectedStart := ctx.StartTime.Add(-30 * time.Second)
	expectedStep := 10000
	rangeSeries, err := rangeOfSeries(ctx, singlePathSpec{
		Values: input,
	})
	require.Nil(t, err)
	expected := common.TestSeries{
		Name: "rangeOfSeries(a,b,c,d)",
		Data: []float64{0, 0, 0, 12, 12, 12, 14, 14, 14, 0, 0, 0},
	}
	common.CompareOutputsAndExpected(t, expectedStep, expectedStart,
		[]common.TestSeries{expected}, rangeSeries.Values)
}

type percentileFunction func(ctx *common.Context, seriesList singlePathSpec, percentile float64) (ts.SeriesList, error)

func testPercentileFunction(t *testing.T, f percentileFunction, expected []common.TestSeriesWithTags) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	startTime := ctx.StartTime
	stepSize := 10000
	inputs := []struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
		tags        map[string]string
	}{
		{
			"foo",
			startTime,
			stepSize,
			[]float64{nan, nan, nan, nan, nan},
			map[string]string{"city": "Maplewood", "state": "NJ"},
		},
		{
			"bar",
			startTime,
			stepSize,
			[]float64{3.0, 2.0, 4.0, nan, 1.0, 6.0, nan, 5.0},
			map[string]string{"title": "24", "star": "Jack Bauer", "rating": "5.0"},
		},
		{
			"baz",
			startTime,
			stepSize,
			[]float64{1.0},
			map[string]string{"name": "Giants", "location": "New York City", "color": "blue"},
		},
	}

	inputSeries := make([]*ts.Series, 0, len(inputs))
	for _, input := range inputs {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		series.Tags = input.tags
		inputSeries = append(inputSeries, series)
	}
	percentile := 40.123
	results, err := f(ctx, singlePathSpec{
		Values: inputSeries,
	}, percentile)
	require.Nil(t, err)
	common.CompareOutputsAndExpectedWithTags(t, stepSize, startTime,
		expected, results.Values)
}

func TestNPercentile(t *testing.T) {
	expected := []common.TestSeriesWithTags{
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "nPercentile(bar, 40.123)",
				Data: []float64{3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0},
			},
			Tags: map[string]string{"title": "24", "star": "Jack Bauer", "rating": "5.0"},
		},
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "nPercentile(baz, 40.123)",
				Data: []float64{1.0},
			},
			Tags: map[string]string{"name": "Giants", "location": "New York City", "color": "blue"},
		},
	}
	testPercentileFunction(t, nPercentile, expected)
}

func TestRemoveAbovePercentile(t *testing.T) {
	nan := math.NaN()
	expected := []common.TestSeriesWithTags{
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "removeAbovePercentile(foo, 40.123)",
				Data: []float64{nan, nan, nan, nan, nan},
			},
			Tags: map[string]string{"city": "Maplewood", "state": "NJ"},
		},
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "removeAbovePercentile(bar, 40.123)",
				Data: []float64{3.0, 2.0, nan, nan, 1.0, nan, nan, nan},
			},
			Tags: map[string]string{"title": "24", "star": "Jack Bauer", "rating": "5.0"},
		},
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "removeAbovePercentile(baz, 40.123)",
				Data: []float64{1.0},
			},
			Tags: map[string]string{"name": "Giants", "location": "New York City", "color": "blue"},
		},
	}

	testPercentileFunction(t, removeAbovePercentile, expected)
}

func TestRemoveBelowPercentile(t *testing.T) {
	nan := math.NaN()

	expected := []common.TestSeriesWithTags{
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "removeBelowPercentile(foo, 40.123)",
				Data: []float64{nan, nan, nan, nan, nan},
			},
			Tags: map[string]string{"city": "Maplewood", "state": "NJ"},
		},
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "removeBelowPercentile(bar, 40.123)",
				Data: []float64{3.0, nan, 4.0, nan, nan, 6.0, nan, 5.0},
			},
			Tags: map[string]string{"title": "24", "star": "Jack Bauer", "rating": "5.0"},
		},
		common.TestSeriesWithTags{
			Series: common.TestSeries{
				Name: "removeBelowPercentile(baz, 40.123)",
				Data: []float64{1.0},
			},
			Tags: map[string]string{"name": "Giants", "location": "New York City", "color": "blue"},
		},
	}

	testPercentileFunction(t, removeBelowPercentile, expected)
}

func testRandomWalkFunctionInternal(t *testing.T, ctx *common.Context, stepSize, expectedLen int) {
	r, err := randomWalkFunction(ctx, "foo", stepSize)
	require.Nil(t, err)

	results := r.Values
	require.Equal(t, 1, len(results))
	require.Equal(t, expectedLen, results[0].Len())
	for i := 0; i < expectedLen; i++ {
		v := results[0].ValueAt(i)
		require.True(t, v >= -0.5 && v < 0.5)
	}
}

func TestRandomWalkFunction(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	ctx.EndTime = ctx.StartTime.Add(1100 * time.Millisecond)
	testRandomWalkFunctionInternal(t, ctx, 1, 2)

	ctx.EndTime = ctx.StartTime.Add(1600 * time.Millisecond)
	testRandomWalkFunctionInternal(t, ctx, 1, 2)
}

func testAggregateLineInternal(t *testing.T, f string, expectedName string, expectedVal float64) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	input := struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		"foo",
		ctx.StartTime,
		10000,
		[]float64{1.0, 2.0, 3.0, 4.0},
	}

	series := ts.NewSeries(
		ctx,
		input.name,
		input.startTime,
		common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
	)

	r, err := aggregateLine(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, f)
	require.Nil(t, err)

	results := r.Values
	require.Equal(t, 1, len(results))
	require.Equal(t, expectedName, results[0].Name())
	require.Equal(t, 2, results[0].Len())
	for i := 0; i < 2; i++ {
		require.Equal(t, expectedVal, results[0].ValueAt(i))
	}
}

func TestAggregateLine(t *testing.T) {
	testAggregateLineInternal(t, "avg", "aggregateLine(foo,2.500)", 2.5)
	testAggregateLineInternal(t, "max", "aggregateLine(foo,4.000)", 4.0)
	testAggregateLineInternal(t, "min", "aggregateLine(foo,1.000)", 1.0)
}

func TestChanged(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	startTime := ctx.StartTime
	stepSize := 10000
	input := struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		"foo",
		startTime,
		stepSize,
		[]float64{1.0, 1.0, 2.0, 3.0, nan, 3.0, nan, 4.0, nan},
	}

	series := ts.NewSeries(
		ctx,
		input.name,
		input.startTime,
		common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
	)

	expected := []common.TestSeries{
		common.TestSeries{
			Name: "changed(foo)",
			Data: []float64{0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0},
		},
	}
	results, err := changed(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	})
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, stepSize, startTime,
		expected, results.Values)
}

func TestMovingMedian(t *testing.T) {
	now := time.Now()
	engine := NewEngine(
		testStorage,
	)
	startTime := now.Add(-3 * time.Minute)
	endTime := now.Add(-time.Minute)
	ctx := common.NewContext(common.ContextOptions{Start: startTime, End: endTime, Engine: engine})
	defer ctx.Close()

	stepSize := 60000
	target := "movingMedian(foo.bar.q.zed, '1min')"
	expr, err := engine.Compile(target)
	require.NoError(t, err)
	res, err := expr.Execute(ctx)
	require.NoError(t, err)
	expected := common.TestSeries{
		Name: "movingMedian(foo.bar.q.zed,\"1min\")",
		Data: []float64{0.0, 0.0},
	}
	common.CompareOutputsAndExpected(t, stepSize, startTime,
		[]common.TestSeries{expected}, res.Values)
}

func TestLegendValue(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	vals := []float64{1.0, 2.0, 3.0, 4.0, math.NaN()}
	input := struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		"foo",
		ctx.StartTime,
		10000,
		vals,
	}

	series := ts.NewSeries(
		ctx,
		input.name,
		input.startTime,
		common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
	)

	results, err := legendValue(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, "avg")
	expected := common.TestSeries{Name: "foo (avg: 2.500)", Data: vals}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, input.stepInMilli, input.startTime,
		[]common.TestSeries{expected}, results.Values)

	results, err = legendValue(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, "last")
	expected = common.TestSeries{Name: "foo (last: 4.000)", Data: vals}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, input.stepInMilli, input.startTime,
		[]common.TestSeries{expected}, results.Values)

	results, err = legendValue(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, "unknown")
	require.NotNil(t, err)
}

func TestCactiStyle(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	stepSize := 10000
	inputs := []struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		{
			"foo",
			ctx.StartTime,
			stepSize,
			[]float64{1.0, 2.0, 3.0, 4.0, math.NaN()},
		},
		{
			"barbaz",
			ctx.StartTime,
			stepSize,
			[]float64{10.0, -5.0, 80.0, 100.0, math.NaN()},
		},
		{
			"test",
			ctx.StartTime,
			stepSize,
			[]float64{math.NaN()},
		},
	}

	inputSeries := make([]*ts.Series, 0, len(inputs))
	for _, input := range inputs {
		series := ts.NewSeries(
			ctx,
			input.name,
			input.startTime,
			common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
		)
		inputSeries = append(inputSeries, series)
	}

	results, err := cactiStyle(ctx, singlePathSpec{
		Values: inputSeries,
	})
	expected := []common.TestSeries{
		{Name: "foo    Current:4.00      Max:4.00      Min:1.00     ", Data: inputs[0].values},
		{Name: "barbaz Current:100.00    Max:100.00    Min:-5.00    ", Data: inputs[1].values},
		{Name: "test   Current:nan       Max:nan       Min:nan      ", Data: inputs[2].values},
	}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, stepSize, ctx.StartTime,
		expected, results.Values)
}

func TestConsolidateBy(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	stepSize := 10000
	input := struct {
		name        string
		startTime   time.Time
		stepInMilli int
		values      []float64
	}{
		"foo",
		ctx.StartTime,
		stepSize,
		[]float64{1.0, 2.0, 3.0, 4.0, math.NaN()},
	}

	series := ts.NewSeries(
		ctx,
		input.name,
		input.startTime,
		common.NewTestSeriesValues(ctx, input.stepInMilli, input.values),
	)

	results, err := consolidateBy(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, "min")
	expected := common.TestSeries{Name: `consolidateBy(foo,"min")`, Data: input.values}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, input.stepInMilli, input.startTime,
		[]common.TestSeries{expected}, results.Values)

	results, err = consolidateBy(ctx, singlePathSpec{
		Values: []*ts.Series{series},
	}, "nonexistent")
	require.NotNil(t, err)
}

func mockSeriesReducer(series *ts.Series) float64 {
	return series.ValueAt(0)
}

func TestOffsetToZero(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	startTime := ctx.StartTime
	stepSize := 10000
	inputs := []struct {
		name     string
		values   []float64
		expected []float64
	}{
		{
			"foo",
			[]float64{nan, nan, nan, nan, nan},
			[]float64{nan, nan, nan, nan, nan},
		},
		{
			"bar",
			[]float64{3.0, 2.0, 4.0, nan, 1.0, 6.0, nan, 5.0},
			[]float64{2.0, 1.0, 3.0, nan, 0.0, 5.0, nan, 4.0},
		},
		{
			"baz",
			[]float64{1.0},
			[]float64{0.0},
		},
	}

	for _, input := range inputs {
		series := ts.NewSeries(
			ctx,
			input.name,
			startTime,
			common.NewTestSeriesValues(ctx, stepSize, input.values),
		)
		results, err := offsetToZero(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		})
		require.NoError(t, err)
		expected := common.TestSeries{
			Name: fmt.Sprintf("offsetToZero(%s)", input.name),
			Data: input.expected,
		}
		common.CompareOutputsAndExpected(t, stepSize, startTime,
			[]common.TestSeries{expected}, results.Values)
	}
}

func TestTimeFunction(t *testing.T) {
	ctx := common.NewTestContext()
	now := time.Now()
	truncatedNow := float64(now.Truncate(time.Second).Unix())
	ctx.StartTime = now
	ctx.EndTime = now.Add(2 * time.Minute)
	defer ctx.Close()

	results, err := timeFunction(ctx, "foo", 30)
	require.NoError(t, err)
	expected := common.TestSeries{
		Name: "foo",
		Data: []float64{truncatedNow, truncatedNow + 30, truncatedNow + 60, truncatedNow + 90},
	}
	common.CompareOutputsAndExpected(t, 30000, now.Truncate(time.Second),
		[]common.TestSeries{expected}, results.Values)
}

func TestTimeShift(t *testing.T) {
	now := time.Now()
	engine := NewEngine(
		testStorage,
	)
	startTime := now.Add(-3 * time.Minute)
	endTime := now.Add(-time.Minute)
	ctx := common.NewContext(common.ContextOptions{
		Start:  startTime,
		End:    endTime,
		Engine: engine,
	})
	defer ctx.Close()

	stepSize := 60000
	target := "timeShift(foo.bar.q.zed, '1min', false)"
	expr, err := engine.Compile(target)
	require.NoError(t, err)
	res, err := expr.Execute(ctx)
	require.NoError(t, err)
	expected := common.TestSeries{
		Name: "timeShift(foo.bar.q.zed, -1min)",
		Data: []float64{0.0, 0.0},
	}
	common.CompareOutputsAndExpected(t, stepSize, startTime,
		[]common.TestSeries{expected}, res.Values)
}

func TestDashed(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	nan := math.NaN()
	startTime := ctx.StartTime
	stepSize := 10000
	inputs := []struct {
		name     string
		values   []float64
		expected []float64
	}{
		{
			"foo",
			[]float64{nan, nan, nan, nan, nan},
			[]float64{nan, nan, nan, nan, nan},
		},
	}

	for _, input := range inputs {
		series := ts.NewSeries(
			ctx,
			input.name,
			startTime,
			common.NewTestSeriesValues(ctx, stepSize, input.values),
		)
		results, err := dashed(ctx, singlePathSpec{
			Values: []*ts.Series{series},
		}, 3.0)
		require.NoError(t, err)
		expected := common.TestSeries{
			Name: fmt.Sprintf("dashed(%s, 3.000)", input.name),
			Data: input.expected,
		}
		common.CompareOutputsAndExpected(t, stepSize, startTime,
			[]common.TestSeries{expected}, results.Values)
		require.Equal(t, "3.000", results.Values[0].GraphOptions["dashed"])
	}
}

func TestThreshold(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	r, err := threshold(ctx, 1.0, "bar", "yellow")
	require.NoError(t, err)

	results := r.Values
	require.Equal(t, 1, len(results))
	require.Equal(t, "bar", results[0].Name())
	require.Equal(t, "yellow", results[0].GraphOptions["color"])

	r, err = threshold(ctx, 1.0, "", "red")
	require.NoError(t, err)

	results = r.Values
	require.Equal(t, 1, len(results))
	require.Equal(t, "1.000", results[0].Name())
	require.Equal(t, "red", results[0].GraphOptions["color"])

	r, err = threshold(ctx, 1.0, "", "")
	require.NoError(t, err)

	results = r.Values
	require.Equal(t, 1, len(results))
	require.Equal(t, "1.000", results[0].Name())
	require.Nil(t, results[0].GraphOptions)
}

func TestFunctionsRegistered(t *testing.T) {
	fnames := []string{
		"abs",
		"absolute",
		"aggregateLine",
		"alias",
		"aliasByMetric",
		"aliasByNode",
		"aliasSub",
		"asPercent",
		"averageAbove",
		"averageSeries",
		"averageSeriesWithWildcards",
		"avg",
		"cactiStyle",
		"changed",
		"consolidateBy",
		"constantLine",
		"countSeries",
		"currentAbove",
		"currentBelow",
		"dashed",
		"derivative",
		"diffSeries",
		"divideSeries",
		"exclude",
		"fallbackSeries",
		"fiveSigma",
		"group",
		"groupByNode",
		"highestAverage",
		"highestCurrent",
		"highestMax",
		"hitcount",
		"holtWintersAberration",
		"holtWintersConfidenceBands",
		"holtWintersForecast",
		"identity",
		"integral",
		"isNonNull",
		"keepLastValue",
		"legendValue",
		"limit",
		"log",
		"logarithm",
		"lowestAverage",
		"lowestCurrent",
		"max",
		"maxSeries",
		"maximumAbove",
		"min",
		"minSeries",
		"minimumAbove",
		"mostDeviant",
		"movingAverage",
		"movingMedian",
		"multiplySeries",
		"nonNegativeDerivative",
		"nPercentile",
		"offset",
		"offsetToZero",
		"perSecond",
		"randomWalk",
		"randomWalkFunction",
		"rangeOfSeries",
		"removeAbovePercentile",
		"removeAboveValue",
		"removeBelowPercentile",
		"removeBelowValue",
		"removeEmptySeries",
		"scale",
		"scaleToSeconds",
		"sortByMaxima",
		"sortByName",
		"sortByTotal",
		"squareRoot",
		"stdev",
		"substr",
		"sum",
		"sumSeries",
		"summarize",
		"threshold",
		"time",
		"timeFunction",
		"timeShift",
		"transformNull",
		"weightedAverage",
	}

	for _, fname := range fnames {
		assert.NotNil(t, findFunction(fname), "could not find function: %s", fname)
	}
}
