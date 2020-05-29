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
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"go.uber.org/zap/zapcore"
)

type mockQuerier struct{}

type mockSeriesSet struct {
	promstorage.SeriesSet
}

func (m mockSeriesSet) Next() bool             { return false }
func (m mockSeriesSet) At() promstorage.Series { return nil }
func (m mockSeriesSet) Err() error             { return nil }

func (mockQuerier) Select(
	sortSeries bool,
	hints *promstorage.SelectHints,
	labelMatchers ...*labels.Matcher,
) (promstorage.SeriesSet, promstorage.Warnings, error) {
	return mockSeriesSet{}, nil, nil
}

func (mockQuerier) LabelValues(name string) ([]string, promstorage.Warnings, error) {
	return nil, nil, errors.New("not implemented")
}

func (mockQuerier) LabelNames() ([]string, promstorage.Warnings, error) {
	return nil, nil, errors.New("not implemented")
}

func (mockQuerier) Close() error {
	return nil
}

type mockQueryable struct{}

func (mockQueryable) Querier(_ context.Context, _, _ int64) (promstorage.Querier, error) {
	return mockQuerier{}, nil
}

func newMockPromQLEngine() *promql.Engine {
	var (
		instrumentOpts = instrument.NewOptions()
		kitLogger      = kitlogzap.NewZapSugarLogger(instrumentOpts.Logger(), zapcore.InfoLevel)
		opts           = promql.EngineOpts{
			Logger:     log.With(kitLogger, "component", "query engine"),
			MaxSamples: 100,
			Timeout:    1 * time.Minute,
		}
	)
	return promql.NewEngine(opts)
}
