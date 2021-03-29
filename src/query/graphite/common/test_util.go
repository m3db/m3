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
	stdcontext "context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
	xtest "github.com/m3db/m3/src/query/graphite/testing"
	"github.com/m3db/m3/src/query/graphite/ts"
	querystorage "github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSeries is used to create a tsdb.timeSeries
type TestSeries struct {
	Name string
	Data []float64
}

// NewTestContext creates a new test context.
func NewTestContext() *Context {
	now := time.Now().Truncate(time.Hour)
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
		i := i // To capture for wrapMsg.
		e := expected[i].Data
		a := actual[i]
		wrapMsg := func(str string) string {
			return fmt.Sprintf("%s\nseries=%d\nexpected=%v\nactual=%v",
				str, i, e, a.SafeValues())
		}
		require.Equal(t, expected[i].Name, a.Name())
		assert.Equal(t, step, a.MillisPerStep(), wrapMsg(a.Name()+
			": MillisPerStep in expected series do not match MillisPerStep in actual"))
		diff := time.Duration(math.Abs(float64(start.Sub(a.StartTime()))))
		assert.True(t, diff < time.Millisecond, wrapMsg(fmt.Sprintf(
			"%s: StartTime in expected series (%v) does not match StartTime in actual (%v), diff %v",
			a.Name(), start, a.StartTime(), diff)))

		require.Equal(t, len(e), a.Len(),
			wrapMsg(a.Name()+
				": length of expected series does not match length of actual"))
		for step := 0; step < a.Len(); step++ {
			v := a.ValueAt(step)
			if math.IsNaN(e[step]) {
				msg := wrapMsg(fmt.Sprintf(
					"%s: invalid value for step %d/%d, should be NaN but is %v",
					a.Name(), 1+step, a.Len(), v))
				assert.True(t, math.IsNaN(v), msg)
			} else if math.IsNaN(v) {
				msg := wrapMsg(fmt.Sprintf(
					"%s: invalid value for step %d/%d, should be %v but is NaN ",
					a.Name(), 1+step, a.Len(), e[step]))
				assert.Fail(t, msg)
			} else {
				msg := wrapMsg(fmt.Sprintf(
					"%s: invalid value for %d/%d",
					a.Name(), 1+step, a.Len()))
				xtest.InDeltaWithNaNs(t, e[step], v, 0.0001, msg)
			}
		}
	}
}

// MovingFunctionStorage is a special test construct for all moving functions
type MovingFunctionStorage struct {
	StepMillis      int
	Bootstrap       []float64
	Values          []float64
	OriginalValues  []SeriesNameAndValues
	BootstrapValues []SeriesNameAndValues
	BootstrapStart  time.Time
}

// SeriesNameAndValues is a series name and a set of values.
type SeriesNameAndValues struct {
	Name   string
	Values []float64
}

// FetchByPath builds a new series from the input path
func (s *MovingFunctionStorage) FetchByPath(
	ctx context.Context,
	path string,
	opts storage.FetchOptions,
) (*storage.FetchResult, error) {
	return s.fetchByIDs(ctx, []string{path}, opts)
}

// FetchByQuery builds a new series from the input query
func (s *MovingFunctionStorage) FetchByQuery(
	ctx context.Context,
	query string,
	opts storage.FetchOptions,
) (*storage.FetchResult, error) {
	return s.fetchByIDs(ctx, []string{query}, opts)
}

// FetchByIDs builds a new series from the input query
func (s *MovingFunctionStorage) fetchByIDs(
	ctx context.Context,
	ids []string,
	opts storage.FetchOptions,
) (*storage.FetchResult, error) {
	if s.Bootstrap == nil && s.Values == nil && s.OriginalValues == nil && s.BootstrapValues == nil {
		return storage.NewFetchResult(ctx, nil, block.NewResultMetadata()), nil
	}

	var (
		// nolint: prealloc
		seriesList []*ts.Series
		// nolint: prealloc
		values []float64
	)
	if opts.StartTime.Equal(s.BootstrapStart) {
		if s.BootstrapValues != nil {
			for _, elem := range s.BootstrapValues {
				series := ts.NewSeries(ctx, elem.Name, opts.StartTime,
					NewTestSeriesValues(ctx, s.StepMillis, elem.Values))
				seriesList = append(seriesList, series)
			}
			return storage.NewFetchResult(ctx, seriesList, block.NewResultMetadata()), nil
		}

		values = append(values, s.Bootstrap...)
		values = append(values, s.Values...)
	} else {
		if s.OriginalValues != nil {
			for _, elem := range s.OriginalValues {
				series := ts.NewSeries(ctx, elem.Name, opts.StartTime,
					NewTestSeriesValues(ctx, s.StepMillis, elem.Values))
				seriesList = append(seriesList, series)
			}
			return storage.NewFetchResult(ctx, seriesList, block.NewResultMetadata()), nil
		}

		values = append(values, s.Values...)
	}

	for _, id := range ids {
		series := ts.NewSeries(ctx, id, opts.StartTime,
			NewTestSeriesValues(ctx, s.StepMillis, values))
		seriesList = append(seriesList, series)
	}

	return storage.NewFetchResult(ctx, seriesList, block.NewResultMetadata()), nil
}

// CompleteTags implements the storage interface.
func (s *MovingFunctionStorage) CompleteTags(
	ctx stdcontext.Context,
	query *querystorage.CompleteTagsQuery,
	opts *querystorage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return nil, fmt.Errorf("not implemented")
}
