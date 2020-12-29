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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/query/block"
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
	m3dbOpts       m3db.Options
	instrumentOpts instrument.Options
	opts           M3WrappedStorageOptions
}

// M3WrappedStorageOptions is the graphite storage options.
type M3WrappedStorageOptions struct {
	AggregateNamespacesAllData                 bool
	ShiftTimeStart                             time.Duration
	ShiftTimeEnd                               time.Duration
	ShiftStepsStart                            int
	ShiftStepsEnd                              int
	ShiftStepsStartWhenAtResolutionBoundary    *int
	ShiftStepsEndWhenAtResolutionBoundary      *int
	ShiftStepsStartWhenEndAtResolutionBoundary *int
	ShiftStepsEndWhenStartAtResolutionBoundary *int
	RenderPartialStart                         bool
	RenderPartialEnd                           bool
	RenderSeriesAllNaNs                        bool
	CompileEscapeAllNotOnlyQuotes              bool
}

type seriesMetadata struct {
	Resolution time.Duration
}

// NewM3WrappedStorage creates a graphite storage wrapper around an m3query
// storage instance.
func NewM3WrappedStorage(
	m3storage storage.Storage,
	m3dbOpts m3db.Options,
	instrumentOpts instrument.Options,
	opts M3WrappedStorageOptions,
) Storage {
	return &m3WrappedStore{
		m3:             m3storage,
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
	if strings.Contains(query, "**") {
		// First add matcher to ensure it's a graphite metric with __g0__ tag.
		hasFirstPathMatcher, err := convertMetricPartToMatcher(0, wildcard)
		if err != nil {
			return nil, err
		}
		// Need to regexp on the entire ID since ** matches over different
		// graphite path dimensions.
		globOpts := graphite.GlobOptions{
			AllowMatchAll: true,
		}
		idRegexp, _, err := graphite.ExtendedGlobToRegexPattern(query, globOpts)
		if err != nil {
			return nil, err
		}
		return models.Matchers{
			hasFirstPathMatcher,
			models.Matcher{
				Type:  models.MatchRegexp,
				Name:  doc.IDReservedFieldName,
				Value: idRegexp,
			},
		}, nil
	}

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
	shiftStepsStart                            int
	shiftStepsEnd                              int
	shiftStepsStartWhenAtResolutionBoundary    *int
	shiftStepsEndWhenAtResolutionBoundary      *int
	shiftStepsStartWhenEndAtResolutionBoundary *int
	shiftStepsEndWhenStartAtResolutionBoundary *int
	renderPartialStart                         bool
	renderPartialEnd                           bool
}

func truncateBoundsToResolution(
	start time.Time,
	end time.Time,
	resolution time.Duration,
	opts truncateBoundsToResolutionOptions,
) (time.Time, time.Time) {
	var (
		truncatedStart            = start.Truncate(resolution)
		truncatedEnd              = end.Truncate(resolution)
		startAtResolutionBoundary = start.Equal(truncatedStart)
		endAtResolutionBoundary   = end.Equal(truncatedEnd)
	)

	// First calculate number of datapoints requested.
	round := math.Floor
	if opts.renderPartialEnd {
		round = math.Ceil
	}
	// If not matched to resolution then return a partial datapoint, unless
	// render partial end is requested in which case return the extra datapoint.
	length := round(float64(end.Sub(start)) / float64(resolution))

	// Now determine start time depending on if in the middle of a step or not.
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

	// Finally calculate end.
	end = start.Add(time.Duration(length) * resolution)

	// Apply shifts.
	var (
		shiftStartAtBoundary        = opts.shiftStepsStartWhenAtResolutionBoundary
		shiftEndAtBoundary          = opts.shiftStepsEndWhenAtResolutionBoundary
		shiftStartWhenEndAtBoundary = opts.shiftStepsStartWhenEndAtResolutionBoundary
		shiftEndWhenStartAtBoundary = opts.shiftStepsEndWhenStartAtResolutionBoundary
		shiftStartOverride          bool
		shiftEndOverride            bool
	)
	if startAtResolutionBoundary {
		if n := shiftStartAtBoundary; n != nil {
			// Apply start boundary shifts which override constant shifts if at boundary.
			start = start.Add(time.Duration(*n) * resolution)
			shiftStartOverride = true
		}
		if n := shiftEndWhenStartAtBoundary; n != nil && !endAtResolutionBoundary {
			// Apply end boundary shifts which override constant shifts if at boundary.
			end = end.Add(time.Duration(*n) * resolution)
			shiftEndOverride = true
		}
	}
	if endAtResolutionBoundary {
		if n := shiftEndAtBoundary; n != nil {
			// Apply end boundary shifts which override constant shifts if at boundary.
			end = end.Add(time.Duration(*n) * resolution)
			shiftEndOverride = true
		}
		if n := shiftStartWhenEndAtBoundary; n != nil && !startAtResolutionBoundary {
			// Apply start boundary shifts which override constant shifts if at boundary.
			start = start.Add(time.Duration(*n) * resolution)
			shiftStartOverride = true
		}
	}

	if !shiftStartOverride {
		// Apply constant shift if no override shift effective.
		start = start.Add(time.Duration(opts.shiftStepsStart) * resolution)
	}
	if !shiftEndOverride {
		// Apply constant shift if no override shift effective.
		end = end.Add(time.Duration(opts.shiftStepsEnd) * resolution)
	}

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
	fetchOptions.Source = fetchOpts.Source

	// NB: ensure single block return.
	fetchOptions.BlockType = models.TypeSingleBlock
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
		shiftStepsStart:                            s.opts.ShiftStepsStart,
		shiftStepsEnd:                              s.opts.ShiftStepsEnd,
		shiftStepsStartWhenAtResolutionBoundary:    s.opts.ShiftStepsStartWhenAtResolutionBoundary,
		shiftStepsEndWhenAtResolutionBoundary:      s.opts.ShiftStepsEndWhenAtResolutionBoundary,
		shiftStepsStartWhenEndAtResolutionBoundary: s.opts.ShiftStepsStartWhenEndAtResolutionBoundary,
		shiftStepsEndWhenStartAtResolutionBoundary: s.opts.ShiftStepsEndWhenStartAtResolutionBoundary,
		renderPartialStart:                         s.opts.RenderPartialStart,
		renderPartialEnd:                           s.opts.RenderPartialEnd,
	}

	series, err := translateTimeseries(ctx, res,
		fetchOpts.StartTime, fetchOpts.EndTime, s.m3dbOpts, truncateOpts)
	if err != nil {
		return nil, err
	}

	return NewFetchResult(ctx, series, res.Metadata), nil
}
