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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"
	xgomock "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queryTestResult struct {
	series   string
	expected string
	max      float64
}

type queryTest struct {
	query   string
	ordered bool
	results []queryTestResult
}

func snapStartToStepSize(t time.Time, stepSize int) time.Time {
	step := time.Duration(stepSize) * time.Millisecond
	if truncated := t.Truncate(step); truncated.Before(t) {
		return t.Add(step)
	}

	return t
}

func testSeries(name string, stepSize int, val float64, opts storage.FetchOptions) *ts.Series {
	ctx := context.New()
	numSteps := int(opts.EndTime.Sub(opts.StartTime)/time.Millisecond) / stepSize
	vals := ts.NewConstantValues(ctx, val, numSteps, stepSize)
	firstPoint := snapStartToStepSize(opts.StartTime, stepSize)
	return ts.NewSeries(ctx, name, firstPoint, vals)
}

func buildTestSeriesFn(
	stepSize int,
	id ...string,
) func(context.Context, string, storage.FetchOptions) (*storage.FetchResult, error) {
	return func(_ context.Context, q string, opts storage.FetchOptions) (*storage.FetchResult, error) {
		series := make([]*ts.Series, 0, len(id))
		for _, name := range id {
			val := testValues[name]
			series = append(series, testSeries(name, stepSize, val, opts))
		}

		return &storage.FetchResult{SeriesList: series}, nil
	}
}

var (
	testValues = map[string]float64{
		"foo.bar.q.zed":      0,
		"foo.bar.g.zed":      1,
		"foo.bar.x.zed":      2,
		"san_francisco.cake": 3,
		"new_york_city.cake": 4,
		"chicago.cake":       5,
		"los_angeles.cake":   6,
	}
)

func newTestStorage(ctrl *gomock.Controller) storage.Storage {
	store := storage.NewMockStorage(ctrl)
	store.EXPECT().FetchByQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(
				ctx context.Context,
				query string,
				opts storage.FetchOptions,
			) (*storage.FetchResult, error) {
				return &storage.FetchResult{}, nil
			})

	return store
}

func TestExecute(t *testing.T) {
	ctrl := xgomock.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)
	engine := NewEngine(store, CompileOptions{})

	tests := []queryTest{
		{"foo.bar.q.zed", true, []queryTestResult{{"foo.bar.q.zed", "foo.bar.q.zed", 0}}},
		{"foo.bar.*.zed", false, []queryTestResult{
			{"foo.bar.q.zed", "foo.bar.q.zed", 0},
			{"foo.bar.g.zed", "foo.bar.g.zed", 1},
			{"foo.bar.x.zed", "foo.bar.x.zed", 2}},
		},
		{"sortByName(aliasByNode(foo.bar.*.zed, 0, 2))", true, []queryTestResult{
			{"foo.bar.g.zed", "foo.g", 1},
			{"foo.bar.q.zed", "foo.q", 0},
			{"foo.bar.x.zed", "foo.x", 2},
		}},
		{"groupByNodes(foo.bar.*.zed, \"sum\")", false, []queryTestResult{
			{"foo.bar.*.zed", "foo.bar.*.zed", 3},
		}},
		{"groupByNodes(foo.bar.*.zed, \"sum\", 2)", false, []queryTestResult{
			{"foo.bar.q.zed", "foo.bar.q.zed", 0},
			{"foo.bar.g.zed", "foo.bar.g.zed", 1},
			{"foo.bar.x.zed", "foo.bar.x.zed", 2},
		}},
	}

	ctx := common.NewContext(common.ContextOptions{Start: time.Now().Add(-1 * time.Hour), End: time.Now(), Engine: engine})
	for _, test := range tests {

		stepSize := 60000
		queries := make([]string, 0, len(test.results))
		for _, r := range test.results {
			queries = append(queries, r.series)
		}

		store.EXPECT().FetchByQuery(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			buildTestSeriesFn(stepSize, queries...))

		expr, err := engine.Compile(test.query)
		require.NoError(t, err)

		results, err := expr.Execute(ctx)
		require.Nil(t, err, "failed to execute %s", test.query)
		require.Equal(t, len(test.results), len(results.Values), "invalid results for %s", test.query)

		for i := range test.results {
			if test.ordered {
				assert.Equal(t, test.results[i].expected, results.Values[i].Name(),
					"invalid result %d for %s", i, test.query)
				assert.Equal(t, test.results[i].max, results.Values[i].CalcStatistics().Max,
					"invalid result %d for %s", i, test.query)
			}
		}
	}
}

func TestTracing(t *testing.T) {
	ctrl := xgomock.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)

	engine := NewEngine(store, CompileOptions{})
	var traces []common.Trace

	ctx := common.NewContext(common.ContextOptions{Start: time.Now().Add(-1 * time.Hour), End: time.Now(), Engine: engine})
	ctx.Trace = func(t common.Trace) {
		traces = append(traces, t)
	}

	stepSize := 60000
	store.EXPECT().FetchByQuery(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		buildTestSeriesFn(stepSize, "foo.bar.q.zed", "foo.bar.g.zed",
			"foo.bar.x.zed"))

	expr, err := engine.Compile("groupByNode(sortByName(aliasByNode(foo.bar.*.zed, 0, 2)), 0, 'sumSeries')")
	require.NoError(t, err)

	_, err = expr.Execute(ctx)
	require.NoError(t, err)

	expectedTraces := []common.Trace{
		{
			ActivityName: "fetch foo.bar.*.zed",
			Outputs:      common.TraceStats{NumSeries: 3}},
		{
			ActivityName: "aliasByNode",
			Inputs:       []common.TraceStats{{NumSeries: 3}},
			Outputs:      common.TraceStats{NumSeries: 3}},
		{
			ActivityName: "sortByName",
			Inputs:       []common.TraceStats{{NumSeries: 3}},
			Outputs:      common.TraceStats{NumSeries: 3}},
		{
			ActivityName: "groupByNode",
			Inputs:       []common.TraceStats{{NumSeries: 3}},
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

func buildEmptyTestSeriesFn() func(context.Context, string, storage.FetchOptions) (*storage.FetchResult, error) {
	return func(_ context.Context, q string, opts storage.FetchOptions) (*storage.FetchResult, error) {
		series := make([]*ts.Series, 0, 0)
		return &storage.FetchResult{SeriesList: series}, nil
	}
}

func TestNilContextShifter(t *testing.T) {
	ctrl := xgomock.NewController(t)
	defer ctrl.Finish()

	store := storage.NewMockStorage(ctrl)

	engine := NewEngine(store, CompileOptions{})

	ctx := common.NewContext(common.ContextOptions{Start: time.Now().Add(-1 * time.Hour), End: time.Now(), Engine: engine})

	store.EXPECT().FetchByQuery(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		buildEmptyTestSeriesFn()).AnyTimes()

	expr, err := engine.Compile("movingSum(foo.bar.q.zed, '30s')")
	require.NoError(t, err)

	_, err = expr.Execute(ctx)
	require.NoError(t, err)
}

func TestRollingMovingSumAndAggregateWithWildcardsAndTransformNull(t *testing.T) {
	ctrl := xgomock.NewController(t)
	defer ctrl.Finish()

	seriesData := []struct {
		id         string
		datapoints []ts.Datapoint
		seed       []ts.Datapoint
	}{
		{
			id: "foo.a.path.count",
			seed: []ts.Datapoint{
				{Timestamp: time.Unix(1625659800, 0), Value: 1.0},
				{Timestamp: time.Unix(1625746260, 0), Value: 1.0},
				{Timestamp: time.Unix(1625832600, 0), Value: 1.0},
				{Timestamp: time.Unix(1625919120, 0), Value: 1.0},
				{Timestamp: time.Unix(1626005640, 0), Value: 1.0},
			},
		},
		{
			id: "foo.b.path.count",
			seed: []ts.Datapoint{
				{Timestamp: time.Unix(1626091740, 0), Value: 1.0},
				{Timestamp: time.Unix(1626184320, 0), Value: 1.0},
			},
		},
	}

	for i := 0; i < 2; i++ {
		for timestamp := 1625605740; timestamp <= 1626210480; timestamp += 60 {
			dp := ts.Datapoint{
				Timestamp: time.Unix(int64(timestamp), 0),
				Value:     math.NaN(),
			}
			for _, seed := range seriesData[i].seed {
				if dp.Timestamp.Equal(seed.Timestamp) {
					dp = seed
					break
				}
			}
			seriesData[i].datapoints = append(seriesData[i].datapoints, dp)
		}
	}

	var (
		min          time.Time
		max          time.Time
		minWithValue time.Time
		step         time.Duration
	)
	for i, v := range seriesData[0].datapoints {
		if i > 0 {
			prev := step
			step = v.Timestamp.Sub(seriesData[0].datapoints[i-1].Timestamp)
			if i > 1 && prev != step {
				require.FailNow(t, fmt.Sprintf("misaligned step: prev=%s, curr=%s", prev, step))
			}
		}

		if max.IsZero() || v.Timestamp.After(max) {
			max = v.Timestamp
		}
		if min.IsZero() || v.Timestamp.Before(min) {
			min = v.Timestamp
		}

		if !math.IsNaN(v.Value) {
			// Record the min timestamp with a value.
			if minWithValue.IsZero() || v.Timestamp.Before(minWithValue) {
				minWithValue = v.Timestamp
			}
		}
	}

	store := storage.NewMockStorage(ctrl)
	store.EXPECT().
		FetchByQuery(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string, opts storage.FetchOptions) (*storage.FetchResult, error) {
			series := make([]*ts.Series, 0, len(seriesData))
			millisPerStep := int(step / time.Millisecond)
			numSteps := int(opts.EndTime.Sub(opts.StartTime) / step)
			for _, data := range seriesData {
				vals := ts.NewValues(ctx, millisPerStep, numSteps)
				idx := 0
				for _, v := range data.datapoints {
					if v.Timestamp.Before(opts.StartTime) {
						continue
					}
					if !v.Timestamp.Before(opts.EndTime) {
						continue
					}
					vals.SetValueAt(idx, v.Value)
					idx++
				}
				newSeries := ts.NewSeries(ctx, data.id, opts.StartTime, vals)
				series = append(series, newSeries)
			}

			return &storage.FetchResult{SeriesList: series}, nil
		}).
		AnyTimes()

	engine := NewEngine(store, CompileOptions{})
	queryRange := 3 * time.Minute
	for end := max; !end.Add(-queryRange).Before(min); end = end.Add(-time.Minute) {
		if !end.After(minWithValue) {
			// Successful test, no ranges were evaluated with a zero value.
			break
		}

		start := end.Add(-queryRange)
		ctx := common.NewContext(common.ContextOptions{
			Start:  start,
			End:    end,
			Engine: engine,
		})

		expr, err := engine.Compile(
			"movingSum(aggregateWithWildcards(transformNull(foo.*.path.count, 0), 'sum', 1), '26h')")
		require.NoError(t, err)

		result, err := expr.Execute(ctx)
		require.NoError(t, err)

		require.Equal(t, 1, result.Len())

		for _, out := range result.Values {
			for _, v := range out.SafeValues() {
				if v < 1 {
					msg := fmt.Sprintf("failed: name=%s, start=%v, end=%v, values=%+v",
						out.Name(), start.Unix(), end.Unix(), out.SafeValues())
					require.FailNow(t, msg)
				}
			}
		}
	}
}
