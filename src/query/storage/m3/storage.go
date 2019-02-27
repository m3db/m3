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
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
	"github.com/m3db/m3x/ident"
	xsync "github.com/m3db/m3x/sync"
)

var (
	errUnaggregatedAndAggregatedDisabled = goerrors.New("fetch options has both " +
		"aggregated and unaggregated namespace lookup disabled")
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

type m3storage struct {
	clusters        Clusters
	readWorkerPool  xsync.PooledWorkerPool
	writeWorkerPool xsync.PooledWorkerPool
	opts            m3db.Options
	nowFn           func() time.Time
	conversionCache *storage.QueryConversionCache
}

// NewStorage creates a new local m3storage instance.
// TODO: consider taking in an iterator pools here.
func NewStorage(
	clusters Clusters,
	readWorkerPool xsync.PooledWorkerPool,
	writeWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
	lookbackDuration time.Duration,
	queryConversionCache *storage.QueryConversionCache,
) (Storage, error) {
	opts := m3db.NewOptions().
		SetTagOptions(tagOptions).
		SetLookbackDuration(lookbackDuration).
		SetConsolidationFunc(consolidators.TakeLast)

	return &m3storage{
		clusters:        clusters,
		readWorkerPool:  readWorkerPool,
		writeWorkerPool: writeWorkerPool,
		opts:            opts,
		nowFn:           time.Now,
		conversionCache: queryConversionCache,
	}, nil
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

	iters, attrs, err := accumulator.FinalResultWithAttrs()
	defer accumulator.Close()
	if err != nil {
		return nil, err
	}

	fetchResult, err := storage.SeriesIteratorsToFetchResult(
		iters,
		s.readWorkerPool,
		false,
		s.opts.TagOptions(),
	)

	if err != nil {
		return nil, err
	}

	if len(fetchResult.SeriesList) != len(attrs) {
		return nil, errMismatchedFetchedLength
	}

	for i := range fetchResult.SeriesList {
		fetchResult.SeriesList[i].SetResolution(attrs[i].Resolution)
	}

	return fetchResult, nil
}

func (s *m3storage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	opts := s.opts
	// If using decoded block, return the legacy path.
	if options.BlockType == models.TypeDecodedBlock {
		fetchResult, err := s.Fetch(ctx, query, options)
		if err != nil {
			return block.Result{}, err
		}

		return storage.FetchResultToBlockResult(fetchResult, query, s.opts.LookbackDuration())
	}

	// If using multiblock, update options to reflect this.
	if options.BlockType == models.TypeMultiBlock {
		opts = opts.
			SetSplitSeriesByBlock(true)
	}

	raw, _, err := s.FetchCompressed(ctx, query, options)
	if err != nil {
		return block.Result{}, err
	}

	bounds := models.Bounds{
		Start:    query.Start,
		Duration: query.End.Sub(query.Start),
		StepSize: query.Interval,
	}

	blocks, err := m3db.ConvertM3DBSeriesIterators(raw, bounds, opts)
	if err != nil {
		return block.Result{}, err
	}

	return block.Result{
		Blocks: blocks,
	}, nil
}

func (s *m3storage) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (encoding.SeriesIterators, Cleanup, error) {
	accumulator, err := s.fetchCompressed(ctx, query, options)
	if err != nil {
		return nil, noop, err
	}

	iters, err := accumulator.FinalResult()
	if err != nil {
		accumulator.Close()
		return nil, noop, err
	}

	return iters, accumulator.Close, nil
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

	m3query, err := storage.FetchQueryToM3Query(query, s.conversionCache)
	if err != nil {
		return nil, err
	}

	// NB(r): Since we don't use a single index we fan out to each
	// cluster that can completely fulfill this range and then prefer the
	// highest resolution (most fine grained) results.
	// This needs to be optimized, however this is a start.
	fanout, namespaces, err := resolveClusterNamespacesForQuery(
		s.nowFn(),
		s.clusters,
		query.Start,
		query.End,
		options.FanoutOptions,
	)

	if err != nil {
		return nil, err
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
		namespace := namespace // Capture var)

		wg.Add(1)
		go func() {
			session := namespace.Session()
			ns := namespace.NamespaceID()
			iters, _, err := session.FetchTagged(ns, m3query, opts)
			// Ignore error from getting iterator pools, since operation
			// will not be dramatically impacted if pools is nil
			result.Add(namespace.Options().Attributes(), iters, err)
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

func (s *m3storage) FetchTags(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	tagResult, cleanup, err := s.SearchCompressed(ctx, query, options)
	defer cleanup()
	if err != nil {
		return nil, err
	}

	metrics := make(models.Metrics, len(tagResult))
	for i, result := range tagResult {
		m, err := storage.FromM3IdentToMetric(result.ID, result.Iter, s.opts.TagOptions())
		if err != nil {
			return nil, err
		}

		metrics[i] = m
	}

	return &storage.SearchResults{
		Metrics: metrics,
	}, nil
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

	// TODO: instead of aggregating locally, have the DB aggregate it before
	// sending results back.
	fetchQuery := &storage.FetchQuery{
		TagMatchers: query.TagMatchers,
		// NB: complete tags matches every tag from the start of time until now
		Start: time.Time{},
		End:   time.Now(),
	}

	results, cleanup, err := s.SearchCompressed(ctx, fetchQuery, options)
	defer cleanup()
	if err != nil {
		return nil, err
	}

	accumulatedTags := storage.NewCompleteTagsResultBuilder(query.CompleteNameOnly)
	// only filter if there are tags to filter on.
	filtering := len(query.FilterNameTags) > 0
	for _, elem := range results {
		it := elem.Iter
		tags := make([]storage.CompletedTag, 0, it.Len())
		for i := 0; it.Next(); i++ {
			tag := it.Current()
			name := tag.Name.Bytes()
			if filtering {
				found := false
				for _, filterName := range query.FilterNameTags {
					if bytes.Equal(filterName, name) {
						found = true
						break
					}
				}

				if !found {
					continue
				}
			}

			tags = append(tags, storage.CompletedTag{
				Name:   name,
				Values: [][]byte{tag.Value.Bytes()},
			})
		}

		if err := elem.Iter.Err(); err != nil {
			return nil, err
		}

		accumulatedTags.Add(&storage.CompleteTagsResult{
			CompleteNameOnly: query.CompleteNameOnly,
			CompletedTags:    tags,
		})
	}

	built := accumulatedTags.Build()
	return &built, nil
}

func (s *m3storage) SearchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) ([]MultiTagResult, Cleanup, error) {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
	}

	m3query, err := storage.FetchQueryToM3Query(query, s.conversionCache)
	if err != nil {
		return nil, noop, err
	}

	var (
		m3opts     = storage.FetchOptionsToM3Options(options, query)
		namespaces = s.clusters.ClusterNamespaces()
		result     = NewMultiFetchTagsResult()
		wg         sync.WaitGroup
	)

	if len(namespaces) == 0 {
		return nil, noop, errNoNamespacesConfigured
	}

	wg.Add(len(namespaces))
	for _, namespace := range namespaces {
		namespace := namespace // Capture var
		go func() {
			session := namespace.Session()
			namespaceID := namespace.NamespaceID()
			iter, _, err := session.FetchTaggedIDs(namespaceID, m3query, m3opts)
			result.Add(iter, err)
			wg.Done()
		}()
	}

	wg.Wait()

	tagResult, err := result.FinalResult()
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
			err = fmt.Errorf("no configured cluster namespace for: retention=%s, resolution=%s",
				attrs.Retention.String(), attrs.Resolution.String())
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
