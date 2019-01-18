package ts

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	xtest "github.com/m3db/m3/src/query/graphite/testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A Datapoint is a datapoint (timestamp, value, optional series) used in testing
type testDatapoint struct {
	SeriesName string
	Timestamp  time.Time
	Value      float64
}

// Datapoints is a set of datapoints
type testDatapoints []testDatapoint

// Shuffle randomizes the set of datapoints
func (pts testDatapoints) Shuffle() {
	for i := len(pts) - 1; i > 0; i-- {
		if j := rand.Intn(i + 1); i != j {
			pts[i], pts[j] = pts[j], pts[i]
		}
	}
}

func TestLcm(t *testing.T) {
	assert.Equal(t, int64(210), Lcm(10, 21))
	assert.Equal(t, int64(210), Lcm(10, -21))
	assert.Equal(t, int64(210), Lcm(-10, 21))
	assert.Equal(t, int64(210), Lcm(-10, -21))
	assert.Equal(t, int64(306), Lcm(18, 17))
	assert.Equal(t, int64(306), Lcm(17, 18))
	assert.Equal(t, int64(0), Lcm(0, 5))
}

func TestGcd(t *testing.T) {
	assert.Equal(t, int64(5), Gcd(5, 10))
	assert.Equal(t, int64(5), Gcd(10, 5))
	assert.Equal(t, int64(5), Gcd(-10, 5))
	assert.Equal(t, int64(5), Gcd(10, -5))
	assert.Equal(t, int64(5), Gcd(-10, -5))
	assert.Equal(t, int64(10), Gcd(10, 10))
	assert.Equal(t, int64(8), Gcd(8, 0))
	assert.Equal(t, int64(8), Gcd(0, 8))
}

func TestAllNaN(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	values := NewValues(ctx, 1000, 30)
	assert.True(t, values.AllNaN())
	values.SetValueAt(10, math.NaN())
	assert.True(t, values.AllNaN())
	values.SetValueAt(20, 100)
	assert.False(t, values.AllNaN())

	assert.True(t, NewConstantValues(ctx, math.NaN(), 1000, 10).AllNaN())
	assert.False(t, NewConstantValues(ctx, 200, 1000, 10).AllNaN())
}

func TestConstantValues(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	series := NewSeries(ctx, "foo", time.Now(), NewConstantValues(ctx, 100, 50, 1000))
	assert.Equal(t, 50, series.Len())
	n := series.ValueAt(10)
	assert.Equal(t, float64(100), n)

	agg := series.CalcStatistics()
	assert.Equal(t, uint(50), agg.Count)
	assert.Equal(t, float64(100), agg.Min)
	assert.Equal(t, float64(100), agg.Max)
	assert.Equal(t, float64(100), agg.Mean)
	assert.Equal(t, float64(0), agg.StdDev)
}

func TestConstantNaNValues(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	series := NewSeries(ctx, "foo", time.Now(), NewConstantValues(ctx, math.NaN(), 50, 1000))
	assert.Equal(t, 50, series.Len())
	n := series.ValueAt(10)
	assert.True(t, math.IsNaN(n))
	assert.False(t, series.IsConsolidationFuncSet())
	series.SetConsolidationFunc(ConsolidationSum.Func())
	assert.True(t, series.IsConsolidationFuncSet())
	xtest.Equalish(t, ConsolidationSum.Func(), series.consolidationFunc)
	agg := series.CalcStatistics()
	assert.Equal(t, uint(0), agg.Count)
	assert.True(t, math.IsNaN(agg.Min))
	assert.True(t, math.IsNaN(agg.Max))
	assert.True(t, math.IsNaN(agg.Mean))
	assert.Equal(t, float64(0), agg.StdDev)
}

func TestInvalidConsolidation(t *testing.T) {
	var (
		ctx     = context.New()
		dummyCF = func(existing, toAdd float64, count int) float64 {
			return existing
		}
		millisPerStep = 100
		start         = time.Now()
		end           = start.Add(-1 * time.Hour)
	)
	NewConsolidation(ctx, start, end, millisPerStep, dummyCF)
}

func TestConsolidation(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	startTime := time.Now()
	endTime := startTime.Add(5 * time.Minute)

	datapoints := testDatapoints{
		{Timestamp: startTime.Add(66 * time.Second), Value: 4.5},
		{Timestamp: startTime.Add(67 * time.Second), Value: 5.0},
		{Timestamp: startTime.Add(5 * time.Second), Value: 65.3},
		{Timestamp: startTime.Add(7 * time.Second), Value: 20.5},
		{Timestamp: startTime.Add(23 * time.Second), Value: 17.5},
		{Timestamp: startTime.Add(74 * time.Second), Value: 20.5},
	}

	consolidation := NewConsolidation(ctx, startTime, endTime, 10*1000, func(a, b float64, count int) float64 {
		return a + b
	})

	for i := range datapoints {
		consolidation.AddDatapoint(datapoints[i].Timestamp, datapoints[i].Value)
	}

	series := consolidation.BuildSeries("foo", Finalize)
	assert.Equal(t, 30, series.Len())
	statistics := series.CalcStatistics()
	assert.Equal(t, float64(9.5), statistics.Min)     // Sum of 66 and 67 second
	assert.Equal(t, float64(85.8), statistics.Max)    // Sum of 5 and 7 seconds
	assert.Equal(t, uint(4), statistics.Count)        // 66 and 67 are combined, 5 and 7 are combined
	assert.Equal(t, float64(33.325), statistics.Mean) // Average of sums
}

type consolidationTest struct {
	name            string
	f               ConsolidationFunc
	stepAggregation ConsolidationFunc
	expectedValues  []float64
}

var (
	consolidationStartTime = time.Now().Truncate(time.Minute)
	consolidationEndTime   = consolidationStartTime.Add(1 * time.Minute)
)

func newConsolidationTestSeries(ctx context.Context) []*Series {
	return []*Series{
		// series1 starts and ends at the same time as the consolidation
		NewSeries(ctx, "a", consolidationStartTime,
			NewConstantValues(ctx, 10, 6, 10000)),

		// series2 starts before the consolidation but ends before the end
		NewSeries(ctx, "b", consolidationStartTime.Add(-30*time.Second),
			NewConstantValues(ctx, 15, 6, 10000)),

		// series3 starts after the consolidation and ends after the end
		NewSeries(ctx, "c", consolidationStartTime.Add(30*time.Second),
			NewConstantValues(ctx, 17, 6, 10000)),

		// series4 has a smaller step size than the consolidation
		NewSeries(ctx, "d", consolidationStartTime,
			NewConstantValues(ctx, 3, 60, 1000)),
	}
}

func TestConsolidateSeries(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	consolidatedSeries := newConsolidationTestSeries(ctx)
	tests := []consolidationTest{
		{"sumMins", Sum, Min, []float64{28, 28, 28, 30, 30, 30}},
		{"minSums", Min, Sum, []float64{10, 10, 10, 10, 10, 10}},
		{"minMins", Min, Min, []float64{3, 3, 3, 3, 3, 3}},
	}

	for _, test := range tests {
		consolidation := NewConsolidation(ctx, consolidationStartTime, consolidationEndTime, 10000, test.f)
		for _, series := range consolidatedSeries {
			consolidation.AddSeries(series, test.stepAggregation)
		}

		results := consolidation.BuildSeries("foo", Finalize)
		require.Equal(t, consolidationStartTime, results.StartTime(), "invalid start time for %s", test.name)
		require.Equal(t, consolidationEndTime, results.EndTime(), "invalid end time for %s", test.name)
		require.Equal(t, 6, results.Len(), "invalid consolidation size for %s", test.name)

		for i := 0; i < results.Len(); i++ {
			value := results.ValueAt(i)
			assert.Equal(t, test.expectedValues[i], value, "invalid value for %d of %s", i, test.name)
		}
	}
}

func TestConsolidationAcrossTimeIntervals(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	expectedResults := []float64{
		10 * 6, // entire range falls in the one minute period
		15 * 3, // last half falls into the one minute period
		17 * 3, // first half fallgs into the one minute period
		3 * 60, // entire range falls, at smaller interval
	}

	consolidatedSeries := newConsolidationTestSeries(ctx)
	for i := range consolidatedSeries {
		series, expected := consolidatedSeries[i], expectedResults[i]
		consolidation := NewConsolidation(ctx, consolidationStartTime, consolidationEndTime, 60000, Sum)
		consolidation.AddSeries(series, Sum)
		result := consolidation.BuildSeries("foo", Finalize)
		assert.Equal(t, consolidationStartTime, result.StartTime(), "incorrect start to %s", series.Name())
		assert.Equal(t, consolidationEndTime, result.EndTime(), "incorrect end to %s", series.Name())
		require.Equal(t, 1, result.Len(), "incorrect # of steps for %s", series.Name())

		value := result.ValueAt(0)
		assert.Equal(t, expected, value, "incorrect value for %s", series.Name())

	}
}

func withValues(vals MutableValues, values []float64) Values {
	for i, n := range values {
		vals.SetValueAt(i, n)
	}
	return vals
}

func TestSlicing(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	tests := []struct {
		input      Values
		begin, end int
		output     Values
	}{
		{
			withValues(NewValues(ctx, 100, 5), []float64{10.0, 20.0, 30.0, 40.0, 50.0}),
			1, 3,
			withValues(NewValues(ctx, 100, 2), []float64{20.0, 30.0}),
		},
		{
			NewConstantValues(ctx, 42.0, 10, 100),
			1, 5,
			NewConstantValues(ctx, 42.0, 4, 100),
		},
	}

	start := time.Now()

	for _, test := range tests {
		input := NewSeries(ctx, "<nil>", start, test.input)
		output, err := input.Slice(test.begin, test.end)
		require.NoError(t, err)

		expect := NewSeries(ctx, "<nil>", input.StartTimeForStep(test.begin), test.output)
		require.Equal(t, output.Len(), expect.Len())

		for step := 0; step < output.Len(); step++ {
			v1 := output.ValueAt(step)
			v2 := expect.ValueAt(step)
			assert.Equal(t, v1, v2)
		}
	}
}

func TestAddSeries(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	seriesStart := time.Now().Add(10000 * time.Millisecond)
	ctxStart := seriesStart.Add(400 * time.Millisecond)
	ctxEnd := ctxStart.Add(7200 * time.Millisecond)
	stepSize := 3600
	values := NewValues(ctx, stepSize, 3)
	for i := 0; i < values.Len(); i++ {
		values.SetValueAt(i, float64(i+1))
	}
	series := NewSeries(ctx, "foo", seriesStart, values)
	consolidation := NewConsolidation(ctx, ctxStart, ctxEnd, stepSize, Avg)
	consolidation.AddSeries(series, Avg)
	consolidated := consolidation.BuildSeries("consolidated", Finalize)
	require.Equal(t, 2, consolidated.Len())
	require.Equal(t, 2.0, consolidated.ValueAt(0))
	require.Equal(t, 3.0, consolidated.ValueAt(1))
}

func TestIntersectAndResize(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	seriesStart := time.Now()
	stepSize := 1000
	values := NewValues(ctx, stepSize, 3)
	for i := 0; i < values.Len(); i++ {
		values.SetValueAt(i, float64(i+1))
	}
	series := NewSeries(ctx, "foo", seriesStart, values)
	series.Tags = map[string]string{"foo": "bar", "biz": "baz"}

	tests := []struct {
		startOffset time.Duration
		endOffset   time.Duration
		newStep     int
	}{
		{
			startOffset: -1 * time.Hour,
			endOffset:   -1 * time.Hour,
			newStep:     stepSize,
		},
		{
			startOffset: 0,
			endOffset:   0,
			newStep:     stepSize,
		},
		{
			startOffset: 0,
			endOffset:   0,
			newStep:     stepSize / 2,
		},
	}

	for _, test := range tests {
		start := seriesStart.Add(test.startOffset)
		end := seriesStart.Add(test.endOffset)
		result, err := series.IntersectAndResize(start, end, test.newStep, series.ConsolidationFunc())
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, start, result.StartTime())
		require.Equal(t, end, result.EndTime())
		require.Equal(t, series.Tags, result.Tags)
		require.Equal(t, series.Specification, result.Specification)
		require.Equal(t, series.Name(), result.Name())
		require.Equal(t, test.newStep, result.MillisPerStep())
	}
}

var (
	benchmarkRange     = 24 * time.Hour
	benchmarkEndTime   = time.Now()
	benchmarkStartTime = benchmarkEndTime.Add(-benchmarkRange)
	benchmarkNumSteps  = NumSteps(benchmarkStartTime, benchmarkEndTime, benchmarkStepInMillis)
)

const (
	benchmarkStepInMillis = 10000
)

func buildBenchmarkDatapoints() testDatapoints {
	datapoints := make(testDatapoints, benchmarkNumSteps)
	for i := range datapoints {
		datapoints[i].Timestamp = benchmarkStartTime.Add(time.Millisecond *
			time.Duration(i*benchmarkStepInMillis))
		datapoints[i].Value = 5
	}

	datapoints.Shuffle()
	return datapoints
}

func BenchmarkUint64Adds(b *testing.B) {
	nan := math.Float64bits(math.NaN())
	datapoints := buildBenchmarkDatapoints()
	for i := 0; i < b.N; i++ {
		values := make([]uint64, len(datapoints))
		for j := 0; j < len(datapoints); j++ {
			values[j] = nan
		}

		for j := 0; j < len(datapoints); j++ {
			startTimeMillis := benchmarkStartTime.UnixNano() / 1000000
			millis := datapoints[j].Timestamp.UnixNano() / 1000000

			step := int(millis-startTimeMillis) / benchmarkStepInMillis
			values[step] = 100 + 2
		}
	}
}

func BenchmarkFloat64Adds(b *testing.B) {
	nan := math.NaN()
	datapoints := buildBenchmarkDatapoints()
	for i := 0; i < b.N; i++ {
		values := make([]float64, len(datapoints))
		for j := 0; j < len(datapoints); j++ {
			values[j] = nan
		}

		for j := 0; j < len(datapoints); j++ {
			startTimeMillis := benchmarkStartTime.UnixNano() / 1000000
			millis := datapoints[j].Timestamp.UnixNano() / 1000000

			step := int(millis-startTimeMillis) / benchmarkStepInMillis
			if math.IsNaN(values[step]) {
				values[step] = 200
			} else {
				values[step] = Sum(values[step], 200, 1)
			}
		}
	}
}

func BenchmarkConsolidation(b *testing.B) {
	ctx := context.New()
	defer ctx.Close()

	datapoints := buildBenchmarkDatapoints()
	for i := 0; i < b.N; i++ {
		consolidation := NewConsolidation(ctx, benchmarkStartTime, benchmarkEndTime, benchmarkStepInMillis, Sum)
		for j := 0; j < len(datapoints); j++ {
			consolidation.AddDatapoint(datapoints[j].Timestamp, datapoints[j].Value)
		}
		consolidation.BuildSeries("foo", Finalize)
	}
}

func BenchmarkConsolidationAddSeries(b *testing.B) {
	ctx := context.New()
	defer ctx.Close()

	c := NewConsolidation(ctx, benchmarkStartTime, benchmarkEndTime, benchmarkStepInMillis, Avg)
	stepsInMillis := benchmarkStepInMillis / 10
	numSteps := int(benchmarkRange/time.Millisecond) / stepsInMillis
	series := NewSeries(ctx, "a", benchmarkStartTime,
		NewConstantValues(ctx, 3.1428, numSteps, stepsInMillis))

	require.Equal(b, benchmarkStartTime, series.StartTime(), "start time not equal")
	require.Equal(b, benchmarkEndTime, series.EndTime(), "end time not equal")

	for i := 0; i < b.N; i++ {
		c.AddSeries(series, Sum)
	}
}

func BenchmarkNewSeries(b *testing.B) {
	ctx := context.New()
	defer ctx.Close()
	for i := 0; i < b.N; i++ {
		NewSeries(ctx, "a", benchmarkStartTime,
			NewConstantValues(ctx, 3.1428, 1, 1000))
	}
}
