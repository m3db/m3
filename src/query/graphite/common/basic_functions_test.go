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

package common

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/x/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	consolidationStartTime = time.Now().Truncate(time.Minute).Add(10 * time.Second)
	consolidationEndTime   = consolidationStartTime.Add(1 * time.Minute)
)

type limitFunc func(series ts.SeriesList, n int) (ts.SeriesList, error)

func validateOutputs(t *testing.T, step int, start time.Time, expected []TestSeries, actual []*ts.Series) {
	require.Equal(t, len(expected), len(actual))

	for i := range expected {
		a, e := actual[i], expected[i].Data

		require.Equal(t, len(e), a.Len())

		for step := 0; step < a.Len(); step++ {
			v := a.ValueAt(step)
			assert.Equal(t, e[step], v, "invalid value for %d", step)
		}

		assert.Equal(t, expected[i].Name, a.Name())
		assert.Equal(t, step, a.MillisPerStep())
		assert.Equal(t, start, a.StartTime())
	}
}

func TestLimitFunctions(t *testing.T) {
	ctx := NewTestContext()
	defer ctx.Close()

	var (
		// Values are not required in this test.
		testInput = []TestSeries{
			{"foo", []float64{}}, {"bar", []float64{}},
			{"baz", []float64{}}, {"qux", []float64{}},
		}

		tests = []struct {
			f      limitFunc
			n      int
			inputs []TestSeries
			output []TestSeries
			err    error
		}{
			{Head, 2, testInput, testInput[:2], nil},
			{Head, 100, testInput, testInput, nil},
			{Head, -2, testInput, nil, ErrNegativeCount},
		}

		startTime = time.Now()
		step      = 100
	)

	for _, test := range tests {
		series := ts.SeriesList{Values: NewTestSeriesList(ctx, startTime, test.inputs, step)}
		output, err := test.f(series, test.n)
		assert.Equal(t, err, test.err)

		validateOutputs(t, step, startTime, test.output, output.Values)
	}
}

func TestNormalize(t *testing.T) {
	ctx, input := NewConsolidationTestSeries(consolidationStartTime, consolidationEndTime, 30*time.Second)
	defer ctx.Close()

	normalized, start, end, step, err := Normalize(ctx, ts.SeriesList{
		Values: input,
	})
	expectedStart := ctx.StartTime.Add(-30 * time.Second)
	expectedEnd := ctx.StartTime.Add(90 * time.Second)
	expectedStep := 10000
	nan := math.NaN()
	require.Nil(t, err)
	require.Equal(t, expectedStart, start)
	require.Equal(t, expectedEnd, end)
	require.Equal(t, expectedStep, step)
	expected := []TestSeries{
		{Name: "a", Data: []float64{nan, nan, nan, 10, 10, 10, 10, 10, 10, nan, nan, nan}},
		{Name: "b", Data: []float64{15, 15, 15, 15, 15, 15, nan, nan, nan, nan, nan, nan}},
		{Name: "c", Data: []float64{nan, nan, nan, nan, nan, nan, 17, 17, 17, 17, 17, 17}},
		{Name: "d", Data: []float64{nan, nan, nan, 3, 3, 3, 3, 3, 3, nan, nan, nan}},
	}

	CompareOutputsAndExpected(t, expectedStep, expectedStart, expected, normalized.Values)
}

func TestParseInterval(t *testing.T) {
	tests := map[string]time.Duration{
		"5s":        time.Second * 5,
		"20sec":     time.Second * 20,
		"60seconds": time.Second * 60,
		"1min":      time.Minute * 1,
		"10min":     time.Minute * 10,
		"2minutes":  time.Minute * 2,
		"3minute":   time.Minute * 3,
		"36h":       time.Hour * 36,
		"9hours":    time.Hour * 9,
		"1hour":     time.Hour * 1,
		"12hr":      time.Hour * 12,
		"1d":        time.Hour * 24,
		"2days":     time.Hour * 24 * 2,
		"1mon":      time.Hour * 24 * 30,
		"4W":        time.Hour * 24 * 7 * 4,
		"40weeks":   time.Hour * 24 * 7 * 40,
		"6months":   time.Hour * 24 * 30 * 6,
		"2y":        time.Hour * 24 * 365 * 2,
		"10years":   time.Hour * 24 * 365 * 10,
		"1w5min":    (time.Hour * 24 * 7) + (time.Minute * 5),
	}

	for s, d := range tests {
		actual, err := ParseInterval(s)
		require.NoError(t, err, "got error parsing interval %s", s)

		assert.Equal(t, d, actual, "invalid parse result for %s", s)
	}

	actual, err := ParseInterval("122222222222222222222222222222s")
	assert.True(t, errors.IsInvalidParams(err))
	assert.Equal(t, time.Duration(0), actual)
}

func TestCount(t *testing.T) {
	ctx, valid := NewConsolidationTestSeries(consolidationStartTime, consolidationEndTime, 30*time.Second)
	defer ctx.Close()

	longCtx := NewContext(ContextOptions{
		Start: consolidationStartTime,
		End:   consolidationStartTime.Add(10 * time.Minute),
	})
	defer longCtx.Close()

	renamer := func(series ts.SeriesList) string {
		return "woot"
	}

	tests := []struct {
		context        *Context
		input          []*ts.Series
		renamer        SeriesListRenamer
		expectedSeries TestSeries
		expectedStart  time.Time
		expectedStep   int
	}{{
		ctx,
		valid,
		renamer,
		TestSeries{
			Name: "woot",
			Data: []float64{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4},
		},
		valid[1].StartTime(),
		valid[1].MillisPerStep(),
	},
		{
			longCtx,
			nil,
			renamer,
			TestSeries{
				"woot",
				[]float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			ctx.StartTime,
			MillisPerMinute,
		},
	}

	for _, test := range tests {
		series := ts.SeriesList{Values: test.input}
		results, err := Count(test.context, series, test.renamer)
		require.Nil(t, err)

		CompareOutputsAndExpected(t, test.expectedStep, test.expectedStart,
			[]TestSeries{test.expectedSeries}, results.Values)
	}
}
