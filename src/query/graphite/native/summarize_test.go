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
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSummarize(t *testing.T) {
	// NB(mmihic): Intentionally not aligned on summarization boundaries
	var (
		start  = time.Unix(131, 0)
		end    = time.Unix(251, 0)
		ctx    = common.NewContext(common.ContextOptions{Start: start, End: end})
		vals   = ts.NewValues(ctx, 10000, 12)
		series = []*ts.Series{ts.NewSeries(ctx, "foo", start, vals)}
	)

	defer ctx.Close()

	// 0+1+2
	// 3+4+5
	// 6+7+8
	// 9+10+11
	for i := 0; i < vals.Len(); i++ {
		vals.SetValueAt(i, float64(i))
	}

	tests := []struct {
		name          string
		interval      string
		fname         string
		alignToFrom   bool
		expectedStart time.Time
		expectedEnd   time.Time
		expectedVals  []float64
	}{
		{"summarize(foo, \"30s\", \"sum\")", "30s", "", false,
			time.Unix(120, 0), time.Unix(270, 0),
			[]float64{1, 9, 18, 27, 11},
		},
		{"summarize(foo, \"30s\", \"sum\", true)", "30s", "", true,
			start, end,
			[]float64{3, 12, 21, 30},
		},
		{"summarize(foo, \"1min\", \"sum\")", "1min", "", false,
			time.Unix(120, 0), time.Unix(300, 0),
			[]float64{10, 45, 11},
		},
		{"summarize(foo, \"1min\", \"sum\", true)", "1min", "", true,
			start, end,
			[]float64{15, 51},
		},
		{"summarize(foo, \"1min\", \"avg\")", "1min", "avg", false,
			time.Unix(120, 0), time.Unix(300, 0),
			[]float64{2, 7.5, 11},
		},
		{"summarize(foo, \"1min\", \"last\")", "1min", "last", false,
			start.Truncate(time.Minute), end.Truncate(time.Minute).Add(time.Minute),
			[]float64{4, 10, 11},
		},
	}

	for _, test := range tests {
		outSeries, err := summarize(ctx, singlePathSpec{
			Values: series,
		}, test.interval, test.fname, test.alignToFrom)
		require.NoError(t, err)
		require.Equal(t, 1, len(outSeries.Values))

		out := outSeries.Values[0]
		assert.Equal(t, test.name, out.Name(), "incorrect name for %s", test.name)
		assert.Equal(t, test.expectedStart, out.StartTime(), "incorrect start for %s", test.name)
		assert.Equal(t, test.expectedEnd, out.EndTime(), "incorrect end for %s", test.name)
		require.Equal(t, len(test.expectedVals), out.Len(), "incorrect len for %s", test.name)

		for i := 0; i < out.Len(); i++ {
			assert.Equal(t, test.expectedVals[i], out.ValueAt(i), "incorrect val %d for %s", i, test.name)
		}
	}

	_, err := summarize(ctx, singlePathSpec{
		Values: series,
	}, "0min", "avg", false)
	require.Error(t, err)
}

func TestSmartSummarize(t *testing.T) {
	var (
		start  = time.Unix(131, 0)
		end    = time.Unix(251, 0)
		ctx    = common.NewContext(common.ContextOptions{Start: start, End: end})
		vals   = ts.NewValues(ctx, 10000, 12)
		series = []*ts.Series{ts.NewSeries(ctx, "foo", start, vals)}
	)

	defer ctx.Close()

	// 0+1+2
	// 3+4+5
	// 6+7+8
	// 9+10+11
	for i := 0; i < vals.Len(); i++ {
		vals.SetValueAt(i, float64(i))
	}

	tests := []struct {
		name          string
		interval      string
		fname         string
		expectedStart time.Time
		expectedEnd   time.Time
		expectedVals  []float64
	}{
		{"smartSummarize(foo, \"30s\", \"sum\")",
			"30s",
			"",
			start, end,
			[]float64{3, 12, 21, 30},
		},
		{"smartSummarize(foo, \"1min\", \"sum\")",
			"1min",
			"",
			start,
			end,
			[]float64{15, 51},
		},
		{"smartSummarize(foo, \"40s\", \"median\")",
			"40s",
			"median",
			start,
			end,
			[]float64{1.5, 5.5, 9.5},
		},
		{"smartSummarize(foo, \"30s\", \"median\")",
			"30s",
			"median",
			start,
			end,
			[]float64{1, 4, 7, 10},
		},
	}

	for _, test := range tests {
		outSeries, err := smartSummarize(ctx, singlePathSpec{
			Values: series,
		}, test.interval, test.fname)
		require.NoError(t, err)
		require.Equal(t, 1, len(outSeries.Values))

		out := outSeries.Values[0]
		assert.Equal(t, test.name, out.Name(), "incorrect name for %s", test.name)
		assert.Equal(t, test.expectedStart, out.StartTime(), "incorrect start for %s", test.name)
		assert.Equal(t, test.expectedEnd, out.EndTime(), "incorrect end for %s", test.name)
		require.Equal(t, len(test.expectedVals), out.Len(), "incorrect len for %s", test.name)

		for i := 0; i < out.Len(); i++ {
			assert.Equal(t, test.expectedVals[i], out.ValueAt(i), "incorrect val %d for %s", i, test.name)
		}
	}

	_, err := smartSummarize(ctx, singlePathSpec{
		Values: series,
	}, "0min", "avg")
	require.Error(t, err)
}

func TestSummarizeInvalidInterval(t *testing.T) {

	var (
		start  = time.Unix(131, 0)
		end    = time.Unix(251, 0)
		ctx    = common.NewContext(common.ContextOptions{Start: start, End: end})
		vals   = ts.NewValues(ctx, 10000, 12)
		series = []*ts.Series{ts.NewSeries(ctx, "foo", start, vals)}
	)

	defer ctx.Close()

	_, err := summarize(ctx, singlePathSpec{
		Values: series,
	}, "0min", "avg", false)
	require.Error(t, err)

	_, err = summarize(ctx, singlePathSpec{
		Values: series,
	}, "-1hour", "avg", false)
	require.Error(t, err)
}
