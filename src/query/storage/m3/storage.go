// Copyright (c) 2018 Uber Technologies, Inc.
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

package m3

import (
	"context"
	goerrors "errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	errUnaggregatedAndAggregatedDisabled = goerrors.New("fetch options has both" +
		" aggregated and unaggregated namespace lookup disabled")
	errNoNamespacesConfigured  = goerrors.New("no namespaces configured")
	errMismatchedFetchedLength = goerrors.New("length of fetched attributes and" +
		" series iterators does not match")
)

type queryFanoutType uint

const (
	namespaceInvalid queryFanoutType = iota
	namespaceCoversAllQueryRange
	namespaceCoversPartialQueryRange
)

func (t queryFanoutType) String() string {
	switch t {
	case namespaceCoversAllQueryRange:
		return "coversAllQueryRange"
	case namespaceCoversPartialQueryRange:
		return "coversPartialQueryRange"
	default:
		return "unknown"
	}
}

type m3storage struct {
	clusters        Clusters
	readWorkerPool  xsync.PooledWorkerPool
	writeWorkerPool xsync.PooledWorkerPool
	opts            m3db.Options
	nowFn           func() time.Time
	logger          *zap.Logger
}

// NewStorage creates a new local m3storage instance.
// TODO: consider taking in an iterator pools here.
func NewStorage(
	clusters Clusters,
	readWorkerPool xsync.PooledWorkerPool,
	writeWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	lookbackDuration time.Duration,
	instrumentOpts instrument.Options,
) (Storage, error) {
	opts := m3db.NewOptions().
		SetTagOptions(tagOptions).
		SetLookbackDuration(lookbackDuration).
		SetConsolidationFunc(consolidators.TakeLast)
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return &m3storage{
		clusters:        clusters,
		readWorkerPool:  readWorkerPool,
		writeWorkerPool: writeWorkerPool,
		opts:            opts,
		nowFn:           time.Now,
		logger:          instrumentOpts.Logger(),
	}, nil
}

func (s *m3storage) ErrorBehavior() storage.ErrorBehavior {
	return storage.BehaviorFail
}

func (s *m3storage) Name() string {
	return "local_store"
}

func (s *m3storage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	accumulator, err := s.fetchCompressed(ctx, query, options)
	if err != nil {
		return nil, err
	}

	result, attrs, err := accumulator.FinalResultWithAttrs()
	defer accumulator.Close()
	if err != nil {
		return nil, err
	}

	enforcer := options.Enforcer
	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	fetchResult, err := storage.SeriesIteratorsToFetchResult(
		result.SeriesIterators,
		s.readWorkerPool,
		false,
		result.Metadata,
		enforcer,
		s.opts.TagOptions(),
		options,
	)

	if err != nil {
		return nil, err
	}

	if len(fetchResult.SeriesList) != len(attrs) {
		return nil, errMismatchedFetchedLength
	}

	if options.IncludeResolution {
		resolutions := make([]int64, 0, len(fetchResult.SeriesList))
		for _, attr := range attrs {
			resolutions = append(resolutions, int64(attr.Resolution))
		}

		fetchResult.Metadata.Resolutions = resolutions
	}

	return fetchResult, nil
}

// FetchResultToBlockResult converts an encoded SeriesIterator fetch result
// into blocks.
func FetchResultToBlockResult(
	result SeriesFetchResult,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
	opts m3db.Options,
) (block.Result, error) {
	// If using multiblock, update options to reflect this.
	if options.BlockType == models.TypeMultiBlock {
		opts = opts.
			SetSplitSeriesByBlock(true)
	}

	start := query.Start
	bounds := models.Bounds{
		Start:    start,
		Duration: query.End.Sub(start),
		StepSize: query.Interval,
	}

	enforcer := options.Enforcer
	if enforcer == nil {
		enforcer = cost.NoopChainedEnforcer()
	}

	blocks, err := m3db.ConvertM3DBSeriesIterators(
		result.SeriesIterators,
		bounds,
		result.Metadata,
		opts,
	)

	if err != nil {
		return block.Result{
			Metadata: block.NewResultMetadata(),
		}, err
	}

	return block.Result{
		Blocks:   blocks,
		Metadata: result.Metadata,
	}, nil
}

func (s *m3storage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	// Override options with whatever is the current specified lookback duration.
	opts := s.opts.SetLookbackDuration(
		options.LookbackDurationOrDefault(s.opts.LookbackDuration()))

	// If using decoded block, return the legacy path.
	if options.BlockType == models.TypeDecodedBlock ||
		options.IncludeExemplars {
		fetchResult, err := s.Fetch(ctx, query, options)
		if err != nil {
			return block.Result{
				Metadata: block.NewResultMetadata(),
			}, err
		}

		return storage.FetchResultToBlockResult(fetchResult, query,
			opts.LookbackDuration(), options.Enforcer)
	}

	result, _, err := s.FetchCompressed(ctx, query, options)
	if err != nil {
		return block.Result{
			Metadata: block.NewResultMetadata(),
		}, err
	}

	return FetchResultToBlockResult(result, query, options, opts)
}

func (s *m3storage) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (SeriesFetchResult, Cleanup, error) {
	accumulator, err := s.fetchCompressed(ctx, query, options)
	if err != nil {
		return SeriesFetchResult{
			Metadata: block.NewResultMetadata(),
		}, noop, err
	}

	result, attrs, err := accumulator.FinalResultWithAttrs()
	if err != nil {
		accumulator.Close()
		return result, noop, err
	}

	if options.IncludeResolution {
		resolutions := make([]int64, 0, len(attrs))
		for _, attr := range attrs {
			resolutions = append(resolutions, int64(attr.Resolution))
		}

		result.Metadata.Resolutions = resolutions
	}

	return result, accumulator.Close, nil
}

// fetches compressed series, returning a MultiFetchResult accumulator
func (s *m3storage) fetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (MultiFetchResult, error) {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m3query, err := storage.FetchQueryToM3Query(query)
	if err != nil {
		return nil, err
	}

	// NB(r): Since we don't use a single index we fan out to each
	// cluster that can completely fulfill this range and then prefer the
	// highest resolution (most fine grained) results.
	// This needs to be optimized, however this is a start.
	fanout, namespaces, err := resolveClusterNamespacesForQuery(
		s.nowFn(),
		query.Start,
		query.End,
		s.clusters,
		options.FanoutOptions,
		options.RestrictFetchOptions,
	)
	if err != nil {
		return nil, err
	}

	debugLog := s.logger.Check(zapcore.DebugLevel,
		"query resolved cluster namespace, will use most granular per result")
	if debugLog != nil {
		for _, n := range namespaces {
			debugLog.Write(zap.String("query", query.Raw),
				zap.String("m3query", m3query.String()),
				zap.Time("start", query.Start),
				zap.Time("end", query.End),
				zap.String("fanoutType", fanout.String()),
				zap.String("namespace", n.NamespaceID().String()),
				zap.String("type", n.Options().Attributes().MetricsType.String()),
				zap.String("retention", n.Options().Attributes().Retention.String()),
				zap.String("resolution", n.Options().Attributes().Resolution.String()),
				zap.Bool("remote", options.Remote),
			)
		}
	}

	var (
		opts = storage.FetchOptionsToM3Options(options, query)
		wg   sync.WaitGroup
	)
	if len(namespaces) == 0 {
		return nil, errNoNamespacesConfigured
	}

	pools, err := namespaces[0].Session().IteratorPools()
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve iterator pools: %v", err)
	}

	result := newMultiFetchResult(fanout, pools)
	for _, namespace := range namespaces {
		namespace := namespace // Capture var
		wg.Add(1)
		go func() {
			session := namespace.Session()
			ns := namespace.NamespaceID()
			iters, exhaustive, err := session.FetchTagged(ns, m3query, opts)
			meta := block.NewResultMetadata()
			meta.Exhaustive = exhaustive
			fetchResult := SeriesFetchResult{
				SeriesIterators: iters,
				Metadata:        meta,
			}

			// Ignore error from getting iterator pools, since operation
			// will not be dramatically impacted if pools is nil
			result.Add(fetchResult, namespace.Options().Attributes(), err)
			wg.Done()
		}()
	}

	wg.Wait()

	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return result, err
}

func (s *m3storage) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	tagResult, cleanup, err := s.SearchCompressed(ctx, query, options)
	defer cleanup()
	if err != nil {
		return nil, err
	}

	metrics := make(models.Metrics, 0, len(tagResult.Tags))
	for _, result := range tagResult.Tags {
		m, err := storage.FromM3IdentToMetric(result.ID,
			result.Iter, s.opts.TagOptions())
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, m)
	}

	return &storage.SearchResults{
		Metrics:  metrics,
		Metadata: tagResult.Metadata,
	}, nil
}

// CompleteTagsCompressed has the same behavior as CompleteTags.
func (s *m3storage) CompleteTagsCompressed(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	return s.CompleteTags(ctx, query, options)
}

func (s *m3storage) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	fetchQuery := &storage.FetchQuery{
		TagMatchers: query.TagMatchers,
	}

	m3query, err := storage.FetchQueryToM3Query(fetchQuery)
	if err != nil {
		return nil, err
	}

	aggOpts := storage.FetchOptionsToAggregateOptions(options, query)
	var (
		nameOnly        = query.CompleteNameOnly
		namespaces      = s.clusters.ClusterNamespaces()
		accumulatedTags = storage.NewCompleteTagsResultBuilder(nameOnly)
		multiErr        syncMultiErrs
		wg              sync.WaitGroup
	)

	debugLog := s.logger.Check(zapcore.DebugLevel,
		"completing tags")
	if debugLog != nil {
		filters := make([]string, len(query.FilterNameTags))
		for i, t := range query.FilterNameTags {
			filters[i] = string(t)
		}

		debugLog.Write(zap.Bool("nameOnly", nameOnly),
			zap.Strings("filterNames", filters),
			zap.String("matchers", query.TagMatchers.String()),
			zap.String("m3query", m3query.String()),
			zap.Time("start", query.Start),
			zap.Time("end", query.End),
			zap.Bool("remote", options.Remote),
		)
	}

	if len(namespaces) == 0 {
		return nil, errNoNamespacesConfigured
	}

	var mu sync.Mutex
	aggIterators := make([]client.AggregatedTagsIterator, 0, len(namespaces))
	defer func() {
		mu.Lock()
		for _, it := range aggIterators {
			it.Finalize()
		}

		mu.Unlock()
	}()

	wg.Add(len(namespaces))
	for _, namespace := range namespaces {
		namespace := namespace // Capture var
		go func() {
			defer wg.Done()
			session := namespace.Session()
			namespaceID := namespace.NamespaceID()
			aggTagIter, exhaustive, err := session.Aggregate(namespaceID, m3query, aggOpts)
			if err != nil {
				multiErr.add(err)
				return
			}

			mu.Lock()
			aggIterators = append(aggIterators, aggTagIter)
			mu.Unlock()

			completedTags := make([]storage.CompletedTag, 0, aggTagIter.Remaining())
			for aggTagIter.Next() {
				name, values := aggTagIter.Current()
				tagValues := make([][]byte, 0, values.Remaining())
				for values.Next() {
					tagValues = append(tagValues, values.Current().Bytes())
				}

				if err := values.Err(); err != nil {
					multiErr.add(err)
					return
				}

				completedTags = append(completedTags, storage.CompletedTag{
					Name:   name.Bytes(),
					Values: tagValues,
				})
			}

			if err := aggTagIter.Err(); err != nil {
				multiErr.add(err)
				return
			}

			metadata := block.NewResultMetadata()
			metadata.Exhaustive = exhaustive
			result := &storage.CompleteTagsResult{
				CompleteNameOnly: query.CompleteNameOnly,
				CompletedTags:    completedTags,
				Metadata:         metadata,
			}

			if err := accumulatedTags.Add(result); err != nil {
				multiErr.add(err)
			}
		}()
	}

	wg.Wait()
	if err := multiErr.lastError(); err != nil {
		return nil, err
	}

	built := accumulatedTags.Build()
	return &built, nil
}

func (s *m3storage) SearchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (TagResult, Cleanup, error) {
	// Check if the query was interrupted.
	tagResult := TagResult{
		Metadata: block.NewResultMetadata(),
	}

	select {
	case <-ctx.Done():
		return tagResult, nil, ctx.Err()
	default:
	}

	m3query, err := storage.FetchQueryToM3Query(query)
	if err != nil {
		return tagResult, noop, err
	}

	var (
		namespaces = s.clusters.ClusterNamespaces()
		m3opts     = storage.FetchOptionsToM3Options(options, query)
		result     = NewMultiFetchTagsResult()
		wg         sync.WaitGroup
	)

	debugLog := s.logger.Check(zapcore.DebugLevel,
		"searching")
	if debugLog != nil {
		debugLog.Write(zap.String("query", query.Raw),
			zap.String("m3_query", m3query.String()),
			zap.Time("start", query.Start),
			zap.Time("end", query.End),
			zap.Bool("remote", options.Remote),
		)
	}

	if len(namespaces) == 0 {
		return tagResult, noop, errNoNamespacesConfigured
	}

	wg.Add(len(namespaces))
	for _, namespace := range namespaces {
		namespace := namespace // Capture var
		go func() {
			session := namespace.Session()
			namespaceID := namespace.NamespaceID()
			iter, exhaustive, err := session.FetchTaggedIDs(namespaceID, m3query, m3opts)
			meta := block.NewResultMetadata()
			meta.Exhaustive = exhaustive
			result.Add(iter, meta, err)
			wg.Done()
		}()
	}

	wg.Wait()

	tagResult, err = result.FinalResult()
	return tagResult, result.Close, err
}

func (s *m3storage) Write(
	ctx context.Context,
	query *storage.WriteQuery,
) error {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if query == nil {
		return errors.ErrNilWriteQuery
	}

	var (
		// TODO: Pool this once an ident pool is setup. We will have
		// to stop calling NoFinalize() below if we do that.
		idBuf = query.Tags.ID()
		id    = ident.BytesID(idBuf)
	)
	// Set id to NoFinalize to avoid cloning it in write operations
	id.NoFinalize()
	tagIterator := storage.TagsToIdentTagIterator(query.Tags)

	if len(query.Datapoints) == 1 {
		// Special case single datapoint because it is common and we
		// can avoid the overhead of a waitgroup, goroutine, multierr,
		// iterator duplication etc.
		return s.writeSingle(
			ctx, query, query.Datapoints[0], id, tagIterator)
	}

	var (
		wg       sync.WaitGroup
		multiErr syncMultiErrs
	)

	for _, datapoint := range query.Datapoints {
		tagIter := tagIterator.Duplicate()
		// capture var
		datapoint := datapoint
		wg.Add(1)
		s.writeWorkerPool.Go(func() {
			if err := s.writeSingle(ctx, query, datapoint, id, tagIter); err != nil {
				multiErr.add(err)
			}

			tagIter.Close()
			wg.Done()
		})
	}

	wg.Wait()
	return multiErr.lastError()
}

func (s *m3storage) Type() storage.Type {
	return storage.TypeLocalDC
}

func (s *m3storage) Close() error {
	return nil
}

func (s *m3storage) writeSingle(
	ctx context.Context,
	query *storage.WriteQuery,
	datapoint ts.Datapoint,
	identID ident.ID,
	iterator ident.TagIterator,
) error {
	var (
		namespace ClusterNamespace
		err       error
	)

	attributes := query.Attributes
	switch attributes.MetricsType {
	case storage.UnaggregatedMetricsType:
		namespace = s.clusters.UnaggregatedClusterNamespace()
	case storage.AggregatedMetricsType:
		attrs := RetentionResolution{
			Retention:  attributes.Retention,
			Resolution: attributes.Resolution,
		}
		var exists bool
		namespace, exists = s.clusters.AggregatedClusterNamespace(attrs)
		if !exists {
			err = fmt.Errorf("no configured cluster namespace for: retention=%s,"+
				" resolution=%s", attrs.Retention.String(), attrs.Resolution.String())
		}
	default:
		metricsType := attributes.MetricsType
		err = fmt.Errorf("invalid write request metrics type: %s (%d)",
			metricsType.String(), uint(metricsType))
	}
	if err != nil {
		return err
	}

	namespaceID := namespace.NamespaceID()
	session := namespace.Session()
	return session.WriteTagged(namespaceID, identID, iterator,
		datapoint.Timestamp, datapoint.Value, query.Unit, query.Annotation)
}
