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

package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

var (
	errSeriesNoResolution = errors.New("series has no resolution set")
)

type m3WrappedStore struct {
	m3             storage.Storage
	enforcer       cost.ChainedEnforcer
	instrumentOpts instrument.Options
}

// NewM3WrappedStorage creates a graphite storage wrapper around an m3query
// storage instance.
func NewM3WrappedStorage(
	m3storage storage.Storage,
	enforcer cost.ChainedEnforcer,
	instrumentOpts instrument.Options,
) Storage {
	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	return &m3WrappedStore{
		m3:             m3storage,
		enforcer:       enforcer,
		instrumentOpts: instrumentOpts,
	}
}

// TranslateQueryToMatchersWithTerminator converts a graphite query to tag
// matcher pairs, and adds a terminator matcher to the end.
func TranslateQueryToMatchersWithTerminator(
	query string,
) (models.Matchers, error) {
	metricLength := graphite.CountMetricParts(query)
	// Add space for a terminator character.
	matchersLength := metricLength + 1
	matchers := make(models.Matchers, matchersLength)
	for i := 0; i < metricLength; i++ {
		metric := graphite.ExtractNthMetricPart(query, i)
		if len(metric) > 0 {
			m, err := convertMetricPartToMatcher(i, metric)
			if err != nil {
				return nil, err
			}

			matchers[i] = m
		} else {
			return nil, fmt.Errorf("invalid matcher format: %s", query)
		}
	}

	// Add a terminator matcher at the end to ensure expansion is terminated at
	// the last given metric part.
	matchers[metricLength] = matcherTerminator(metricLength)
	return matchers, nil
}

// GetQueryTerminatorTagName will return the name for the terminator matcher in
// the given pattern. This is useful for filtering out any additional results.
func GetQueryTerminatorTagName(query string) []byte {
	metricLength := graphite.CountMetricParts(query)
	return graphite.TagName(metricLength)
}

func translateQuery(query string, opts FetchOptions) (*storage.FetchQuery, error) {
	matchers, err := TranslateQueryToMatchersWithTerminator(query)
	if err != nil {
		return nil, err
	}

	return &storage.FetchQuery{
		Raw:         query,
		TagMatchers: matchers,
		Start:       opts.StartTime,
		End:         opts.EndTime,
		// NB: interval is not used for initial consolidation step from the storage
		// so it's fine to use default here.
		Interval: time.Duration(0),
	}, nil
}

func translateTimeseries(
	ctx xctx.Context,
	result *storage.FetchResult,
	start, end time.Time,
) ([]*ts.Series, error) {
	m3list := result.SeriesList
	series := make([]*ts.Series, len(m3list))
	resolutions := result.Metadata.Resolutions
	if len(series) != len(resolutions) {
		return nil, fmt.Errorf("number of timeseries %d does not match number of "+
			"resolutions %d", len(series), len(resolutions))
	}

	for i, m3series := range m3list {
		resolution := time.Duration(resolutions[i])
		if resolution <= 0 {
			return nil, errSeriesNoResolution
		}

		length := int(end.Sub(start) / resolution)
		millisPerStep := int(resolution / time.Millisecond)
		values := ts.NewValues(ctx, millisPerStep, length)
		for _, datapoint := range m3series.Values().Datapoints() {
			index := int(datapoint.Timestamp.Sub(start) / resolution)
			if index < 0 || index >= length {
				// Outside of range requested
				continue
			}
			values.SetValueAt(index, datapoint.Value)
		}

		name := string(m3series.Name())
		series[i] = ts.NewSeries(ctx, name, start, values)
	}

	return series, nil
}

func (s *m3WrappedStore) FetchByQuery(
	ctx xctx.Context, query string, opts FetchOptions,
) (*FetchResult, error) {
	m3query, err := translateQuery(query, opts)
	if err != nil {
		// NB: error here implies the query cannot be translated; empty set expected
		// rather than propagating an error.
		logger := logging.WithContext(ctx.RequestContext(), s.instrumentOpts)
		logger.Info("could not translate query, returning empty results",
			zap.String("query", query))
		return &FetchResult{
			SeriesList: []*ts.Series{},
			Metadata:   block.NewResultMetadata(),
		}, nil
	}

	m3ctx, cancel := context.WithTimeout(ctx.RequestContext(), opts.Timeout)
	defer cancel()
	fetchOptions := storage.NewFetchOptions()
	fetchOptions.Limit = opts.Limit
	perQueryEnforcer := s.enforcer.Child(cost.QueryLevel)
	defer perQueryEnforcer.Close()

	fetchOptions.IncludeResolution = true
	fetchOptions.Enforcer = perQueryEnforcer
	fetchOptions.FanoutOptions = &storage.FanoutOptions{
		FanoutUnaggregated:        storage.FanoutForceDisable,
		FanoutAggregated:          storage.FanoutDefault,
		FanoutAggregatedOptimized: storage.FanoutForceDisable,
	}

	m3result, err := s.m3.Fetch(m3ctx, m3query, fetchOptions)
	if err != nil {
		return nil, err
	}

	series, err := translateTimeseries(ctx, m3result,
		opts.StartTime, opts.EndTime)
	if err != nil {
		return nil, err
	}

	return NewFetchResult(ctx, series, m3result.Metadata), nil
}
