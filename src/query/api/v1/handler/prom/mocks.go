package prom

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/kit/log"
	kitlogzap "github.com/go-kit/kit/log/zap"
	"github.com/m3db/m3/src/x/instrument"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
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

func (mockQuerier) Select(*promstorage.SelectParams, ...*promlabels.Matcher) (promstorage.SeriesSet, promstorage.Warnings, error) {
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
			Logger:        log.With(kitLogger, "component", "query engine"),
			MaxConcurrent: 10,
			MaxSamples:    100,
			Timeout:       1 * time.Minute,
		}
	)
	return promql.NewEngine(opts)
}
