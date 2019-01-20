package native

import (
	"testing"
	"time"

	// "code.uber.internal/infra/statsdex/tsdb/mocktsdb"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/storage"

	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queryTestResult struct {
	name string
	max  float64
}

type queryTest struct {
	query   string
	ordered bool
	results []queryTestResult
}

var (
	testValues = map[string]float64{
		"foo.bar.q.zed":       0,
		"foo.bar.g.zed":       1,
		"foo.bar.x.zed":       2,
		"san_francisco.uberx": 3,
		"new_york_city.uberx": 4,
		"chicago.uberx":       5,
		"los_angeles.uberx":   6,
	}

	testPolicy = policy.NewStoragePolicy(10*time.Second, xtime.Second, 48*time.Hour)
	// testTSDB    = makeTSDB(testPolicy)
	testStorage storage.Storage //= nil
	//  local.NewLocalStorage(local.Options{
	// 	Database:       testTSDB,
	// 	Workers:        workers,
	// 	Scope:          metrics.None,
	// 	PolicyResolver: resolver.NewStaticResolver(testIndex, testPolicy),
	// })
)

// TODO arnikola reenable
func testExecute(t *testing.T) {
	engine := NewEngine(
		testStorage,
	)
	tests := []queryTest{
		{"foo.bar.q.zed", true, []queryTestResult{{"foo.bar.q.zed", 0}}},
		{"foo.bar.*.zed", false, []queryTestResult{
			{"foo.bar.q.zed", 0},
			{"foo.bar.g.zed", 1},
			{"foo.bar.x.zed", 2}},
		},
		{"sortByName(aliasByNode(foo.bar.*.zed, 0, 2))", true, []queryTestResult{
			{"foo.g", 1},
			{"foo.q", 0},
			{"foo.x", 2},
		}},
	}

	ctx := common.NewContext(common.ContextOptions{Start: time.Now().Add(-1 * time.Hour), End: time.Now(), Engine: engine})
	for _, test := range tests {
		expr, err := engine.Compile(test.query)
		require.Nil(t, err)

		results, err := expr.Execute(ctx)
		require.Nil(t, err, "failed to execute %s", test.query)
		require.Equal(t, len(test.results), len(results.Values), "invalid results for %s", test.query)

		for i := range test.results {
			if test.ordered {
				assert.Equal(t, test.results[i].name, results.Values[i].Name(),
					"invalid result %d for %s", i, test.query)
				assert.Equal(t, test.results[i].max, results.Values[i].CalcStatistics().Max,
					"invalid result %d for %s", i, test.query)
			}
		}
	}
}

// TODO arnikola reenable
func testTracing(t *testing.T) {
	engine := NewEngine(
		testStorage,
	)
	var traces []common.Trace

	ctx := common.NewContext(common.ContextOptions{Start: time.Now().Add(-1 * time.Hour), End: time.Now(), Engine: engine})
	ctx.Trace = func(t common.Trace) {
		traces = append(traces, t)
	}

	expr, err := engine.Compile("groupByNode(sortByName(aliasByNode(foo.bar.*.zed, 0, 2)), 0, 'sumSeries')")
	require.NoError(t, err)

	_, err = expr.Execute(ctx)
	require.NoError(t, err)

	expectedTraces := []common.Trace{
		common.Trace{
			ActivityName: "fetch foo.bar.*.zed",
			Outputs:      common.TraceStats{NumSeries: 3}},
		common.Trace{
			ActivityName: "aliasByNode",
			Inputs:       []common.TraceStats{common.TraceStats{NumSeries: 3}},
			Outputs:      common.TraceStats{NumSeries: 3}},
		common.Trace{
			ActivityName: "sortByName",
			Inputs:       []common.TraceStats{common.TraceStats{NumSeries: 3}},
			Outputs:      common.TraceStats{NumSeries: 3}},
		common.Trace{
			ActivityName: "groupByNode",
			Inputs:       []common.TraceStats{common.TraceStats{NumSeries: 3}},
			Outputs:      common.TraceStats{NumSeries: 1}},
	}
	require.Equal(t, len(expectedTraces), len(traces))
	for i, expected := range expectedTraces {
		trace := traces[i]
		assert.Equal(t, expected.ActivityName, trace.ActivityName, "incorrect name for trace %d", i)
		assert.Equal(t, expected.Inputs, trace.Inputs, "incorrect inputs for trace %d", i)
		assert.Equal(t, expected.Outputs, trace.Outputs, "incorrect outputs for trace %d", i)
	}
}

// func makeTSDB(policy policy.StoragePolicy) tsdb.Database {
// 	var (
// 		now      = time.Now().Truncate(time.Second * 10)
// 		testTSDB = nil //FIXME mocktsdb.New()
// 		ctx      = context.New()
// 	)

// 	defer ctx.Close()

// 	for name, val := range testValues {
// 		for t := now.Add(-time.Hour * 2); t.Before(now.Add(time.Hour)); t = t.Add(time.Second * 10) {
// 			testTSDB.WriteRaw(ctx, name, t, val, policy)
// 		}
// 	}

// 	return testIndex, testTSDB
// }
