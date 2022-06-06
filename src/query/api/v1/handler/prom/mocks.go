// Copyright (c) 2020 Uber Technologies, Inc.
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

package prom

import (
	"context"
	"errors"
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/go-kit/kit/log"
	kitlogzap "github.com/go-kit/kit/log/zap"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"go.uber.org/zap/zapcore"
)

type mockQuerier struct {
	mockOptions
}

type mockSeriesSet struct {
	mockOptions
	promstorage.SeriesSet
}

func (m *mockSeriesSet) Next() bool                     { return false }
func (m *mockSeriesSet) At() promstorage.Series         { return nil }
func (m *mockSeriesSet) Err() error                     { return nil }
func (m *mockSeriesSet) Warnings() promstorage.Warnings { return nil }

func (q *mockQuerier) Select(
	sortSeries bool,
	hints *promstorage.SelectHints,
	labelMatchers ...*labels.Matcher,
) promstorage.SeriesSet {
	if q.mockOptions.selectFn != nil {
		return q.mockOptions.selectFn(sortSeries, hints, labelMatchers...)
	}
	return &mockSeriesSet{mockOptions: q.mockOptions}
}

func (*mockQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, promstorage.Warnings, error) {
	return nil, nil, errors.New("not implemented")
}

func (*mockQuerier) LabelNames() ([]string, promstorage.Warnings, error) {
	return nil, nil, errors.New("not implemented")
}

func (*mockQuerier) Close() error {
	return nil
}

type mockOptions struct {
	selectFn func(
		sortSeries bool,
		hints *promstorage.SelectHints,
		labelMatchers ...*labels.Matcher,
	) promstorage.SeriesSet
}

type mockQueryable struct {
	mockOptions
}

func (q *mockQueryable) Querier(_ context.Context, _, _ int64) (promstorage.Querier, error) {
	return &mockQuerier{mockOptions: q.mockOptions}, nil
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

func newMockPromQLEngine() *promql.Engine {
	var (
		instrumentOpts = instrument.NewOptions()
		kitLogger      = kitlogzap.NewZapSugarLogger(instrumentOpts.Logger(), zapcore.InfoLevel)
		opts           = promql.EngineOpts{
			Logger:     log.With(kitLogger, "component", "query engine"),
			MaxSamples: 100,
			Timeout:    1 * time.Minute,
			NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 {
				return durationMilliseconds(1 * time.Minute)
			},
		}
	)
	return promql.NewEngine(opts)
}
