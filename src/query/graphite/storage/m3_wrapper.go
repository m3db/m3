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
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	xctx "github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/graphite"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts/m3db"
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
	m3dbOpts       m3db.Options
	instrumentOpts instrument.Options
	opts           M3WrappedStorageOptions
}

// M3WrappedStorageOptions is the graphite storage options.
type M3WrappedStorageOptions struct {
	AggregateNamespacesAllData bool
	ShiftTimeStart             time.Duration
	ShiftTimeEnd               time.Duration
	ShiftStepsStart            int
	ShiftStepsEnd              int
	RenderPartialStart         bool
	RenderPartialEnd           bool
}

type seriesMetadata struct {
	Resolution time.Duration
}

// NewM3WrappedStorage creates a graphite storage wrapper around an m3query
// storage instance.
func NewM3WrappedStorage(
	m3storage storage.Storage,
	enforcer cost.ChainedEnforcer,
	m3dbOpts m3db.Options,
	instrumentOpts instrument.Options,
	opts M3WrappedStorageOptions,
) Storage {
	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	return &m3WrappedStore{
		m3:             m3storage,
		enforcer:       enforcer,
		m3dbOpts:       m3dbOpts,
		instrumentOpts: instrumentOpts,
		opts:           opts,
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

func translateQuery(
	query string,
	fetchOpts FetchOptions,
	opts M3WrappedStorageOptions,
) (*storage.FetchQuery, error) {
	matchers, err := TranslateQueryToMatchersWithTerminator(query)
	if err != nil {
		return nil, err
	}

	// Apply any shifts.
	fetchOpts.StartTime = fetchOpts.StartTime.Add(opts.ShiftTimeStart)
	fetchOpts.EndTime = fetchOpts.EndTime.Add(opts.ShiftTimeEnd)

	return &storage.FetchQuery{
		Raw:         query,
		TagMatchers: matchers,
		Start:       fetchOpts.StartTime,
		End:         fetchOpts.EndTime,
		// NB: interval is not used for initial consolidation step from the storage
		// so it's fine to use default here.
		Interval: time.Duration(0),
	}, nil
}

type truncateBoundsToResolutionOptions struct {
	shiftStepsStart    int
	shiftStepsEnd      int
	renderPartialStart bool
	renderPartialEnd   bool
}

func truncateBoundsToResolution(
	start time.Time,
	end time.Time,
	resolution time.Duration,
	opts truncateBoundsToResolutionOptions,
) (time.Time, time.Time) {
	truncatedStart := start.Truncate(resolution)
	// NB: if truncated start matches start, it's already valid.
	if !start.Equal(truncatedStart) {
		if opts.renderPartialStart {
			// Otherwise if we include partial start then set to truncated.
			start = truncatedStart
		} else {
			// Else we snap to the next step.
			start = truncatedStart.Add(resolution)
		}
	}

	truncatedEnd := end.Truncate(resolution)
	if !end.Equal(truncatedEnd) {
		if opts.renderPartialEnd {
			// Otherwise if we include partial end then set to truncated + 1.
			end = truncatedEnd.Add(resolution)
		} else {
			// Else we snap to the truncated end.
			end = truncatedEnd
		}
	}

	// Apply any shifts.
	start = start.Add(time.Duration(opts.shiftStepsStart) * resolution)
	end = end.Add(time.Duration(opts.shiftStepsEnd) * resolution)
	return start, end
}

func translateTimeseries(
	ctx xctx.Context,
	result block.Result,
	start, end time.Time,
	m3dbOpts m3db.Options,
	truncateOpts truncateBoundsToResolutionOptions,
) ([]*ts.Series, error) {
	if len(result.Blocks) == 0 {
		return []*ts.Series{}, nil
	}

	bl := result.Blocks[0]
	defer bl.Close()

	iter, err := bl.SeriesIter()
	if err != nil {
		return nil, err
	}

	resolutions := result.Metadata.Resolutions
	seriesMetas := iter.SeriesMeta()
	if len(seriesMetas) != len(resolutions) {
		return nil, fmt.Errorf("number of timeseries %d does not match number of "+
			"resolutions %d", len(seriesMetas), len(resolutions))
	}

	seriesMetadataMap := newSeriesMetadataMap(seriesMetadataMapOptions{
		InitialSize: iter.SeriesCount(),
	})

	for i, meta := range seriesMetas {
		seriesMetadataMap.SetUnsafe(meta.Name, seriesMetadata{
			Resolution: resolutions[i],
		}, seriesMetadataMapSetUnsafeOptions{
			NoCopyKey:     true,
			NoFinalizeKey: true,
		})
	}

	var (
		results     []*ts.Series
		resultsLock sync.Mutex
	)
	processor := m3dbOpts.BlockSeriesProcessor()
	err = processor.Process(bl, m3dbOpts, m3db.BlockSeriesProcessorFn(func(
		iter block.SeriesIter,
	) error {
		series, err := translateTimeseriesFromIter(ctx, iter,
			start, end, seriesMetadataMap, truncateOpts)
		if err != nil {
			return err
		}

		resultsLock.Lock()
		defer resultsLock.Unlock()

		if len(results) == 0 {
			// Don't grow slice, can just take ref.
			results = series
		} else {
			results = append(results, series...)
		}

		return nil
	}))
	if err != nil {
		return nil, err
	}
	return results, nil
}

func translateTimeseriesFromIter(
	ctx xctx.Context,
	iter block.SeriesIter,
	queryStart, queryEnd time.Time,
	seriesMetadataMap *seriesMetadataMap,
	opts truncateBoundsToResolutionOptions,
) ([]*ts.Series, error) {
	seriesMetas := iter.SeriesMeta()
	series := make([]*ts.Series, 0, len(seriesMetas))
	for idx := 0; iter.Next(); idx++ {
		meta, ok := seriesMetadataMap.Get(seriesMetas[idx].Name)
		if !ok {
			return nil, fmt.Errorf("series meta for series missing: %s", seriesMetas[idx].Name)
		}

		resolution := time.Duration(meta.Resolution)
		if resolution <= 0 {
			return nil, errSeriesNoResolution
		}

		start, end := truncateBoundsToResolution(queryStart, queryEnd, resolution, opts)
		length := int(end.Sub(start) / resolution)
		millisPerStep := int(resolution / time.Millisecond)
		values := ts.NewValues(ctx, millisPerStep, length)

		m3series := iter.Current()
		dps := m3series.Datapoints()
		for _, datapoint := range dps.Datapoints() {
			ts := datapoint.Timestamp
			if ts.Before(start) {
				// Outside of range requested.
				continue
			}

			if !ts.Before(end) {
				// No more valid datapoints.
				break
			}

			index := int(datapoint.Timestamp.Sub(start) / resolution)
			values.SetValueAt(index, datapoint.Value)
		}

		name := string(seriesMetas[idx].Name)
		series = append(series, ts.NewSeries(ctx, name, start, values))
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return series, nil
}

func (s *m3WrappedStore) FetchByQuery(
	ctx xctx.Context, query string, fetchOpts FetchOptions,
) (*FetchResult, error) {
	m3query, err := translateQuery(query, fetchOpts, s.opts)
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

	m3ctx, cancel := context.WithTimeout(ctx.RequestContext(), fetchOpts.Timeout)
	defer cancel()
	fetchOptions := storage.NewFetchOptions()
	fetchOptions.SeriesLimit = fetchOpts.Limit
	perQueryEnforcer := s.enforcer.Child(cost.QueryLevel)
	defer perQueryEnforcer.Close()

	// NB: ensure single block return.
	fetchOptions.BlockType = models.TypeSingleBlock
	fetchOptions.Enforcer = perQueryEnforcer
	fetchOptions.FanoutOptions = &storage.FanoutOptions{
		FanoutUnaggregated:        storage.FanoutForceDisable,
		FanoutAggregated:          storage.FanoutDefault,
		FanoutAggregatedOptimized: storage.FanoutForceDisable,
	}
	if s.opts.AggregateNamespacesAllData {
		// NB(r): If aggregate namespaces house all the data, we can do a
		// default optimized fanout where we only query the namespaces
		// that contain the data for the ranges we are querying for.
		fetchOptions.FanoutOptions.FanoutAggregatedOptimized = storage.FanoutDefault
	}

	res, err := s.m3.FetchBlocks(m3ctx, m3query, fetchOptions)
	if err != nil {
		return nil, err
	}

	if blockCount := len(res.Blocks); blockCount > 1 {
		return nil, fmt.Errorf("expected at most one block, received %d", blockCount)
	}

	truncateOpts := truncateBoundsToResolutionOptions{
		shiftStepsStart:    s.opts.ShiftStepsStart,
		shiftStepsEnd:      s.opts.ShiftStepsEnd,
		renderPartialStart: s.opts.RenderPartialStart,
		renderPartialEnd:   s.opts.RenderPartialEnd,
	}

	series, err := translateTimeseries(ctx, res,
		fetchOpts.StartTime, fetchOpts.EndTime, s.m3dbOpts, truncateOpts)
	if err != nil {
		return nil, err
	}

	return NewFetchResult(ctx, series, res.Metadata), nil
}
