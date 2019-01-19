package native

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	consolidationStartTime = time.Now().Truncate(time.Minute).Add(10 * time.Second)
	consolidationEndTime   = consolidationStartTime.Add(1 * time.Minute)
)

func newConsolidationTestSeries() (*common.Context, []*ts.Series) {
	ctx := common.NewContext(common.ContextOptions{Start: consolidationStartTime, End: consolidationEndTime})

	testSeries := []*ts.Series{
		ts.NewSeries(ctx, "a", consolidationStartTime,
			ts.NewConstantValues(ctx, 10, 6, 10000)),
		ts.NewSeries(ctx, "b", consolidationStartTime.Add(-30*time.Second),
			ts.NewConstantValues(ctx, 15, 6, 10000)),
		ts.NewSeries(ctx, "c", consolidationStartTime.Add(30*time.Second),
			ts.NewConstantValues(ctx, 17, 6, 10000)),
		ts.NewSeries(ctx, "d", consolidationStartTime,
			ts.NewConstantValues(ctx, 3, 60, 1000)),
	}

	return ctx, testSeries
}

func testAggregatedSeries(
	t *testing.T,
	f func(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error),
	ev1, ev2, ev3, ev4 float64,
	errorMessage string,
) {
	ctx, consolidationTestSeries := newConsolidationTestSeries()
	defer ctx.Close()

	input := ts.SeriesList{Values: consolidationTestSeries}

	r, err := f(ctx, multiplePathSpecs(input))
	require.Nil(t, err)

	series := r.Values
	require.Equal(t, 1, len(series))

	require.Equal(t, consolidationTestSeries[1].StartTime(), series[0].StartTime())
	require.Equal(t, consolidationTestSeries[2].EndTime(), series[0].EndTime())
	require.Equal(t, 12, series[0].Len())
	require.Equal(t, 10000, series[0].MillisPerStep())
	for i := 0; i < 3; i++ {
		n := series[0].ValueAt(i)
		assert.Equal(t, ev1, n, errorMessage, i)
	}
	for i := 3; i < 6; i++ {
		n := series[0].ValueAt(i)
		assert.Equal(t, ev2, n, errorMessage, i)
	}
	for i := 6; i < 9; i++ {
		n := series[0].ValueAt(i)
		assert.Equal(t, ev3, n, errorMessage, i)
	}
	for i := 9; i < 12; i++ {
		n := series[0].ValueAt(i)
		assert.Equal(t, ev4, n, errorMessage, i)
	}

	// nil input -> nil output
	for _, in := range [][]*ts.Series{nil, []*ts.Series{}} {
		series, err := f(ctx, multiplePathSpecs(ts.SeriesList{
			Values: in,
		}))
		require.Nil(t, err)
		require.Equal(t, in, series.Values)
	}

	// single input -> same output
	singleSeries := []*ts.Series{consolidationTestSeries[0]}
	r, err = f(ctx, multiplePathSpecs(ts.SeriesList{
		Values: singleSeries,
	}))
	require.Nil(t, err)

	series = r.Values
	require.Equal(t, singleSeries[0].Len(), series[0].Len())
	for i := 0; i < series[0].Len(); i++ {
		assert.Equal(t, singleSeries[0].ValueAt(i), series[0].ValueAt(i))
	}
}

func TestMinSeries(t *testing.T) {
	testAggregatedSeries(t, minSeries, 15.0, 3.0, 3.0, 17.0, "invalid min value for step %d")
}

func TestMaxSeries(t *testing.T) {
	testAggregatedSeries(t, maxSeries, 15.0, 15.0, 17.0, 17.0, "invalid max value for step %d")
}

func TestSumSeries(t *testing.T) {
	testAggregatedSeries(t, func(ctx *common.Context, series multiplePathSpecs) (ts.SeriesList, error) {
		return sumSeries(ctx, series)
	}, 15.0, 28.0, 30.0, 17.0, "invalid sum value for step %d")
}

type mockEngine struct {
	fn func(
		ctx context.Context,
		query string,
		start, end time.Time,
		localOnly, useCache, useM3DB bool,
		timeout time.Duration,
	) (*storage.FetchResult, error)
}

func (e mockEngine) FetchByQuery(
	ctx context.Context,
	query string,
	start, end time.Time,
	localOnly, useCache, useM3DB bool,
	timeout time.Duration,
) (*storage.FetchResult, error) {
	return e.fn(ctx, query, start, end, localOnly, useCache, useM3DB, timeout)
}

func TestVariadicSumSeries(t *testing.T) {
	expr, err := compile("sumSeries(foo.bar.*, foo.baz.*)")
	require.NoError(t, err)

	ctx := common.NewTestContext()
	ctx.Engine = mockEngine{fn: func(
		ctx context.Context,
		query string,
		start, end time.Time,
		localOnly, useCache, useM3DB bool,
		timeout time.Duration,
	) (*storage.FetchResult, error) {
		switch query {
		case "foo.bar.*":
			return storage.NewFetchResult(ctx, []*ts.Series{
				ts.NewSeries(ctx, "foo.bar.a", start, ts.NewConstantValues(ctx, 1, 3, 1000)),
				ts.NewSeries(ctx, "foo.bar.b", start, ts.NewConstantValues(ctx, 2, 3, 1000)),
			}), nil
		case "foo.baz.*":
			return storage.NewFetchResult(ctx, []*ts.Series{
				ts.NewSeries(ctx, "foo.baz.a", start, ts.NewConstantValues(ctx, 3, 3, 1000)),
				ts.NewSeries(ctx, "foo.baz.b", start, ts.NewConstantValues(ctx, 4, 3, 1000)),
			}), nil
		}
		return nil, fmt.Errorf("unexpected query: %s", query)
	}}

	r, err := expr.Execute(ctx)
	require.NoError(t, err)

	require.Equal(t, 1, r.Len())
	assert.Equal(t, []float64{10, 10, 10}, r.Values[0].SafeValues())
}

func TestDiffSeries(t *testing.T) {
	testAggregatedSeries(t, diffSeries, -15.0, -8.0, -10.0, -17.0, "invalid diff value for step %d")
}

func TestMultiplySeries(t *testing.T) {
	testAggregatedSeries(t, multiplySeries, 15.0, 450.0, 510.0, 17.0, "invalid product value for step %d")
}

func TestAverageSeries(t *testing.T) {
	testAggregatedSeries(t, averageSeries, 15.0, 28.0/3, 10.0, 17.0, "invalid avg value for step %d")
}

func TestDivideSeries(t *testing.T) {
	ctx, consolidationTestSeries := newConsolidationTestSeries()
	defer ctx.Close()

	// multiple series, different start/end times
	nan := math.NaN()
	series, err := divideSeries(ctx, singlePathSpec{
		Values: consolidationTestSeries[0:2],
	}, singlePathSpec{
		Values: consolidationTestSeries[2:3],
	})
	require.Nil(t, err)
	expected := []common.TestSeries{
		{
			Name: "divideSeries(a,c)",
			Data: []float64{nan, nan, nan, 0.5882, 0.5882, 0.5882, nan, nan, nan},
		},
		{
			Name: "divideSeries(b,c)",
			Data: []float64{nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan},
		},
	}

	common.CompareOutputsAndExpected(t, 10000, consolidationStartTime,
		[]common.TestSeries{expected[0]}, []*ts.Series{series.Values[0]})
	common.CompareOutputsAndExpected(t, 10000, consolidationStartTime.Add(-30*time.Second),
		[]common.TestSeries{expected[1]}, []*ts.Series{series.Values[1]})

	// different millisPerStep, same start/end times
	series, err = divideSeries(ctx, singlePathSpec{
		Values: consolidationTestSeries[0:1],
	}, singlePathSpec{
		Values: consolidationTestSeries[3:4],
	})
	require.Nil(t, err)
	expected = []common.TestSeries{
		{
			Name: "divideSeries(a,d)",
			Data: []float64{3.3333, 3.3333, 3.3333, 3.3333, 3.33333, 3.3333},
		},
	}
	common.CompareOutputsAndExpected(t, 10000, consolidationStartTime,
		[]common.TestSeries{expected[0]}, []*ts.Series{series.Values[0]})

	// error - multiple divisor series
	series, err = divideSeries(ctx, singlePathSpec{
		Values: consolidationTestSeries,
	}, singlePathSpec{
		Values: consolidationTestSeries,
	})
	require.Error(t, err)
}

func TestAverageSeriesWithWildcards(t *testing.T) {
	ctx, _ := newConsolidationTestSeries()
	defer ctx.Close()

	input := []common.TestSeries{
		common.TestSeries{"web.host-1.avg-response.value", []float64{70.0, 20.0, 30.0, 40.0, 50.0}},
		common.TestSeries{"web.host-2.avg-response.value", []float64{20.0, 30.0, 40.0, 50.0, 60.0}},
		common.TestSeries{"web.host-3.avg-response.value", []float64{30.0, 40.0, 80.0, 60.0, 70.0}},
		common.TestSeries{"web.host-4.num-requests.value", []float64{10.0, 10.0, 15.0, 10.0, 15.0}},
	}
	expected := []common.TestSeries{
		common.TestSeries{"web.avg-response", []float64{40.0, 30.0, 50.0, 50.0, 60.0}},
		common.TestSeries{"web.num-requests", []float64{10.0, 10.0, 15.0, 10.0, 15.0}},
	}

	start := consolidationStartTime
	step := 12000
	timeSeries := generateSeriesList(ctx, start, input, step)
	output, err := averageSeriesWithWildcards(ctx, singlePathSpec{
		Values: timeSeries,
	}, 1, 3)
	require.NoError(t, err)
	sort.Sort(TimeSeriesPtrVector(output.Values))
	common.CompareOutputsAndExpected(t, step, start, expected, output.Values)
}

func TestSumSeriesWithWildcards(t *testing.T) {
	var (
		start, _ = time.Parse(time.RFC1123, "Mon, 27 Jul 2015 19:41:19 GMT")
		end, _   = time.Parse(time.RFC1123, "Mon, 27 Jul 2015 19:43:19 GMT")
		ctx      = common.NewContext(common.ContextOptions{Start: start, End: end})
		inputs   = []*ts.Series{
			ts.NewSeries(ctx, "servers.foo-1.pod1.status.500", start,
				ts.NewConstantValues(ctx, 2, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-2.pod1.status.500", start,
				ts.NewConstantValues(ctx, 4, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-3.pod1.status.500", start,
				ts.NewConstantValues(ctx, 6, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-1.pod2.status.500", start,
				ts.NewConstantValues(ctx, 8, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-2.pod2.status.500", start,
				ts.NewConstantValues(ctx, 10, 12, 10000)),

			ts.NewSeries(ctx, "servers.foo-1.pod1.status.400", start,
				ts.NewConstantValues(ctx, 20, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-2.pod1.status.400", start,
				ts.NewConstantValues(ctx, 30, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-3.pod2.status.400", start,
				ts.NewConstantValues(ctx, 40, 12, 10000)),
		}
	)
	defer ctx.Close()

	outSeries, err := sumSeriesWithWildcards(ctx, singlePathSpec{
		Values: inputs,
	}, 1, 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(outSeries.Values))

	outSeries, _ = sortByName(ctx, singlePathSpec(outSeries))

	expectedOutputs := []struct {
		name      string
		sumOfVals float64
	}{
		{"servers.status.400", 90 * 12},
		{"servers.status.500", 30 * 12},
	}

	for i, expected := range expectedOutputs {
		series := outSeries.Values[i]
		assert.Equal(t, expected.name, series.Name())
		assert.Equal(t, expected.sumOfVals, series.SafeSum())
	}
}

func TestGroupByNode(t *testing.T) {
	var (
		start, _ = time.Parse(time.RFC1123, "Mon, 27 Jul 2015 19:41:19 GMT")
		end, _   = time.Parse(time.RFC1123, "Mon, 27 Jul 2015 19:43:19 GMT")
		ctx      = common.NewContext(common.ContextOptions{Start: start, End: end})
		inputs   = []*ts.Series{
			ts.NewSeries(ctx, "servers.foo-1.pod1.status.500", start,
				ts.NewConstantValues(ctx, 2, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-2.pod1.status.500", start,
				ts.NewConstantValues(ctx, 4, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-3.pod1.status.500", start,
				ts.NewConstantValues(ctx, 6, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-1.pod2.status.500", start,
				ts.NewConstantValues(ctx, 8, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-2.pod2.status.500", start,
				ts.NewConstantValues(ctx, 10, 12, 10000)),

			ts.NewSeries(ctx, "servers.foo-1.pod1.status.400", start,
				ts.NewConstantValues(ctx, 20, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-2.pod1.status.400", start,
				ts.NewConstantValues(ctx, 30, 12, 10000)),
			ts.NewSeries(ctx, "servers.foo-3.pod2.status.400", start,
				ts.NewConstantValues(ctx, 40, 12, 10000)),
		}
	)
	defer ctx.Close()

	type result struct {
		name      string
		sumOfVals float64
	}

	tests := []struct {
		fname           string
		node            int
		expectedResults []result
	}{
		{"avg", 4, []result{
			{"400", ((20 + 30 + 40) / 3) * 12},
			{"500", ((2 + 4 + 6 + 8 + 10) / 5) * 12},
		}},
		{"max", 2, []result{
			{"pod1", 30 * 12},
			{"pod2", 40 * 12},
		}},
		{"min", -1, []result{
			{"400", 20 * 12},
			{"500", 2 * 12},
		}},
	}

	for _, test := range tests {
		outSeries, err := groupByNode(ctx, singlePathSpec{
			Values: inputs,
		}, test.node, test.fname)
		require.NoError(t, err)
		require.Equal(t, len(test.expectedResults), len(outSeries.Values))

		outSeries, _ = sortByName(ctx, singlePathSpec(outSeries))

		for i, expected := range test.expectedResults {
			series := outSeries.Values[i]
			assert.Equal(t, expected.name, series.Name(),
				"wrong name for %d %s (%d)", test.node, test.fname, i)
			assert.Equal(t, expected.sumOfVals, series.SafeSum(),
				"wrong result for %d %s (%d)", test.node, test.fname, i)
		}
	}
}

func TestWeightedAverage(t *testing.T) {
	ctx, _ := newConsolidationTestSeries()
	defer ctx.Close()

	means := []common.TestSeries{
		common.TestSeries{"web.host-1.avg-response.mean", []float64{70.0, 20.0, 30.0, 0.0, 50.0}},
		common.TestSeries{"web.host-2.avg-response.mean", []float64{20.0, 30.0, 40.0, 50.0, 60.0}},
		common.TestSeries{"web.host-3.avg-response.mean", []float64{20.0, 30.0, 40.0, 50.0, 60.0}}, // no match
	}
	counts := []common.TestSeries{
		common.TestSeries{"web.host-1.avg-response.count", []float64{1, 2, 3, 4, 5}},
		common.TestSeries{"web.host-2.avg-response.count", []float64{10, 20, 30, 40, 50}},
		common.TestSeries{"web.host-4.avg-response.count", []float64{10, 20, 30, 40, 50}}, // no match
	}
	expected := []common.TestSeries{
		common.TestSeries{"weightedAverage", []float64{24.5454, 29.0909, 39.0909, 45.4545, 59.0909}},
	}

	// normal series
	start := consolidationStartTime
	step := 12000
	values := ts.SeriesList{Values: generateSeriesList(ctx, start, means, step)}
	weights := ts.SeriesList{Values: generateSeriesList(ctx, start, counts, step)}
	output, err := weightedAverage(ctx, singlePathSpec(values), singlePathSpec(weights), 1)
	require.NoError(t, err)
	sort.Sort(TimeSeriesPtrVector(output.Values))
	common.CompareOutputsAndExpected(t, step, start, expected, output.Values)

	// one series as input, should return the same as output no matter what the weight
	values = ts.SeriesList{Values: generateSeriesList(ctx, start, means[:1], step)}
	weights = ts.SeriesList{Values: generateSeriesList(ctx, start, counts[:1], step)}
	output, err = weightedAverage(ctx, singlePathSpec(values), singlePathSpec(weights), 1)
	require.NoError(t, err)
	common.CompareOutputsAndExpected(t, step, start,
		[]common.TestSeries{{"weightedAverage", means[0].Data}}, output.Values)

	// different steps should lead to error -- not supported yet
	values = ts.SeriesList{Values: generateSeriesList(ctx, start, means, step)}
	weights = ts.SeriesList{Values: generateSeriesList(ctx, start, counts, step*2)}
	output, err = weightedAverage(ctx, singlePathSpec(values), singlePathSpec(weights), 1)
	require.EqualError(t, err, "different step sizes in input series not supported")
}

func TestCountSeries(t *testing.T) {
	ctx, input := newConsolidationTestSeries()
	defer ctx.Close()

	results, err := countSeries(ctx, multiplePathSpecs(ts.SeriesList{
		Values: input,
	}))
	expected := common.TestSeries{
		Name: "countSeries(a,b,c,d)",
		Data: []float64{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4},
	}
	require.Nil(t, err)
	common.CompareOutputsAndExpected(t, input[1].MillisPerStep(), input[1].StartTime(),
		[]common.TestSeries{expected}, results.Values)
}
