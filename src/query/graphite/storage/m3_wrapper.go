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
	"time"

	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	m3ts "github.com/m3db/m3/src/query/ts"
)

var (
	errSeriesNoResolution = errors.New("series has no resolution set")
)

type m3WrappedStore struct {
	m3 storage.Storage
}

// NewM3WrappedStorage creates a graphite storage wrapper around an m3query
// storage instance.
func NewM3WrappedStorage(m3storage storage.Storage) Storage {
	return &m3WrappedStore{m3: m3storage}
}

// TranslateQueryToMatchers converts a graphite query to tag matcher pairs.
func TranslateQueryToMatchers(query string) models.Matchers {
	metricLength := graphite.CountMetricParts(query)
	matchers := make(models.Matchers, metricLength)
	for i := 0; i < metricLength; i++ {
		metric := graphite.ExtractNthMetricPart(query, i)
		if len(metric) > 0 {
			matchers[i] = convertMetricPartToMatcher(i, metric)
		}
	}

	return matchers
}

// GetQueryTerminatorTagName will return the name for the terminator matcher in
// the given pattern. This is useful for filtering out any additional results.
func GetQueryTerminatorTagName(query string) []byte {
	metricLength := graphite.CountMetricParts(query)
	return graphite.TagName(metricLength)
}

func translateQuery(query string, opts FetchOptions) *storage.FetchQuery {
	metricLength := graphite.CountMetricParts(query)
	matchers := make(models.Matchers, metricLength+1)
	for i := 0; i < metricLength; i++ {
		metric := graphite.ExtractNthMetricPart(query, i)
		if len(metric) > 0 {
			matchers[i] = convertMetricPartToMatcher(i, metric)
		}
	}

	// Add a terminator matcher at the end to ensure expansion is terminated at
	// the last given metric part.
	matchers[metricLength] = matcherTerminator(metricLength)
	return &storage.FetchQuery{
		Raw:         query,
		TagMatchers: matchers,
		Start:       opts.StartTime,
		End:         opts.EndTime,
		// NB: interval is not used for initial consolidation step from the storage
		// so it's fine to use default here.
		Interval: time.Duration(0),
	}
}

func translateTimeseries(
	ctx xctx.Context,
	m3list m3ts.SeriesList,
	start, end time.Time,
) ([]*ts.Series, error) {
	series := make([]*ts.Series, len(m3list))
	for i, m3series := range m3list {
		resolution := m3series.Resolution()
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

		name := m3series.Name()
		if tags := m3series.Tags; tags.Len() > 0 {
			// Need to flatten the name back into graphite format
			newName, err := convertTagsToMetricName(tags)
			if err != nil {
				return nil, err
			}

			name = newName
		}

		series[i] = ts.NewSeries(ctx, name, start, values)
	}

	return series, nil
}

func (s *m3WrappedStore) FetchByQuery(
	ctx xctx.Context, query string, opts FetchOptions,
) (*FetchResult, error) {
	m3query := translateQuery(query, opts)
	m3ctx, cancel := context.WithTimeout(ctx.RequestContext(), opts.Timeout)
	defer cancel()

	fetchOptions := storage.NewFetchOptions()
	fetchOptions.FanoutOptions = &storage.FanoutOptions{
		FanoutUnaggregated:        storage.FanoutForceDisable,
		FanoutAggregated:          storage.FanoutForceEnable,
		FanoutAggregatedOptimized: storage.FanoutForceEnable,
	}

	m3result, err := s.m3.Fetch(m3ctx, m3query, fetchOptions)
	if err != nil {
		return nil, err
	}

	series, err := translateTimeseries(ctx, m3result.SeriesList,
		opts.StartTime, opts.EndTime)
	if err != nil {
		return nil, err
	}

	return NewFetchResult(ctx, series), nil
}
