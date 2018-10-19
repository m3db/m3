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
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/ident"
	xsync "github.com/m3db/m3x/sync"
)

var (
	errNoNamespacesConfigured = goerrors.New("no namespaces configured")
)

type queryFanoutType uint

const (
	namespaceCoversAllQueryRange queryFanoutType = iota
	namespaceCoversPartialQueryRange
)

type m3storage struct {
	tagOptions      models.TagOptions
	clusters        Clusters
	readWorkerPool  xsync.PooledWorkerPool
	writeWorkerPool xsync.PooledWorkerPool
	nowFn           func() time.Time
}

// NewStorage creates a new local m3storage instance.
func NewStorage(
	clusters Clusters,
	readWorkerPool xsync.PooledWorkerPool,
	writeWorkerPool xsync.PooledWorkerPool,
	tagOptions models.TagOptions,
) Storage {
	return &m3storage{
		tagOptions:      tagOptions,
		clusters:        clusters,
		readWorkerPool:  readWorkerPool,
		writeWorkerPool: writeWorkerPool,
		nowFn:           time.Now,
	}
}

func (s *m3storage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	raw, cleanup, err := s.FetchCompressed(ctx, query, options)
	defer cleanup()
	if err != nil {
		return nil, err
	}

	return storage.SeriesIteratorsToFetchResult(raw, s.readWorkerPool, false, s.tagOptions)
}

func (s *m3storage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	fetchResult, err := s.Fetch(ctx, query, options)
	if err != nil {
		return block.Result{}, err
	}

	return storage.FetchResultToBlockResult(fetchResult, query)
}

func (s *m3storage) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (encoding.SeriesIterators, Cleanup, error) {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, noop, ctx.Err()
	case <-options.KillChan:
		return nil, noop, errors.ErrQueryInterrupted
	default:
	}

	m3query, err := storage.FetchQueryToM3Query(query)
	if err != nil {
		return nil, noop, err
	}

	// NB(r): Since we don't use a single index we fan out to each
	// cluster that can completely fulfill this range and then prefer the
	// highest resolution (most fine grained) results.
	// This needs to be optimized, however this is a start.
	fanout, namespaces, err := s.resolveClusterNamespacesForQuery(query.Start, query.End)
	if err != nil {
		return nil, noop, err
	}

	var (
		opts = storage.FetchOptionsToM3Options(options, query)
		wg   sync.WaitGroup
	)
	if len(namespaces) == 0 {
		return nil, noop, errNoNamespacesConfigured
	}

	pools, err := namespaces[0].Session().IteratorPools()
	if err != nil {
		return nil, noop, fmt.Errorf("unable to retrieve iterator pools: %v", err)
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

	iters, err := result.FinalResult()
	if err != nil {
		result.Close()
		return nil, noop, err
	}

	return iters, result.Close, nil
}

func (s *m3storage) FetchTags(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	tagResult, cleanup, err := s.SearchCompressed(ctx, query, options)
	if err != nil {
		return nil, err
	}

	metrics := make(models.Metrics, len(tagResult))
	for i, result := range tagResult {
		m, err := storage.FromM3IdentToMetric(result.ID, result.Iter, s.tagOptions)
		if err != nil {
			return nil, err
		}

		metrics[i] = m
	}

	cleanup()
	return &storage.SearchResults{
		Metrics: metrics,
	}, nil
}

func (s *m3storage) SearchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) ([]MultiTagResult, Cleanup, error) {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, noop, ctx.Err()
	case <-options.KillChan:
		return nil, noop, errors.ErrQueryInterrupted
	default:
	}

	m3query, err := storage.FetchQueryToM3Query(query)
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
		buf   = make([]byte, 0, query.Tags.IDLen())
		idBuf = query.Tags.IDMarshalTo(buf)
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
	return multiErr.finalError()
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

// resolveClusterNamespacesForQuery returns the namespaces that need to be
// fanned out to depending on the query time and the namespaces configured.
func (s *m3storage) resolveClusterNamespacesForQuery(
	start time.Time,
	end time.Time,
) (queryFanoutType, ClusterNamespaces, error) {
	now := s.nowFn()

	unaggregated := s.clusters.UnaggregatedClusterNamespace()
	unaggregatedRetention := unaggregated.Options().Attributes().Retention
	unaggregatedStart := now.Add(-1 * unaggregatedRetention)
	if unaggregatedStart.Before(start) || unaggregatedStart.Equal(start) {
		// Highest resolution is unaggregated, return if it can fulfill it
		return namespaceCoversAllQueryRange, ClusterNamespaces{unaggregated}, nil
	}

	// First determine if any aggregated clusters span the whole query range, if
	// so that's the most optimal strategy, choose the most granular resolution
	// that can and fan out to any partial aggregated namespaces that may holder
	// even more granular resolutions
	var r reusedAggregatedNamespaceSlices
	r = s.aggregatedNamespaces(r, func(namespace ClusterNamespace) bool {
		// Include only if can fulfill the entire time range of the query
		clusterStart := now.Add(-1 * namespace.Options().Attributes().Retention)
		return clusterStart.Before(start) || clusterStart.Equal(start)
	})

	if len(r.completeAggregated) > 0 {
		// Return the most granular completed aggregated namespace and
		// any potentially more granular partial aggregated namespaces
		sort.Stable(ClusterNamespacesByResolutionAsc(r.completeAggregated))

		// Take most granular complete aggregated namespace
		result := r.completeAggregated[:1]
		completedAttrs := result[0].Options().Attributes()

		// Take any finer grain partially aggregated namespaces that
		// may contain a matching metric
		for _, n := range r.partialAggregated {
			if n.Options().Attributes().Resolution >= completedAttrs.Resolution {
				// Not more granular
				continue
			}
			result = append(result, n)
		}

		return namespaceCoversAllQueryRange, result, nil
	}

	// No complete aggregated namespaces can definitely fulfill the query,
	// so take the longest retention completed aggregated namespace to return
	// as much data as possible, along with any partially aggregated namespaces
	// that have either same retention and lower resolution or longer retention
	// than the complete aggregated namespace
	r = s.aggregatedNamespaces(r, nil)

	if len(r.completeAggregated) == 0 {
		// Absolutely no complete aggregated namespaces, need to fanout to all
		// partial aggregated namespaces as well as the unaggregated cluster
		// as we have no idea who has the longest retention
		result := append(r.partialAggregated, unaggregated)
		return namespaceCoversPartialQueryRange, result, nil
	}

	// Return the longest retention aggregated namespace and
	// any potentially more granular or longer retention partial
	// aggregated namespaces
	sort.Stable(sort.Reverse(ClusterNamespacesByRetentionAsc(r.completeAggregated)))

	// Take longest retention complete aggregated namespace or the unaggregated
	// cluster if that is longer than the longest aggregated namespace
	result := r.completeAggregated[:1]
	completedAttrs := result[0].Options().Attributes()
	if completedAttrs.Retention <= unaggregatedRetention {
		// If the longest aggregated cluster for some reason has lower retention
		// than the unaggregated cluster then we prefer the unaggregated cluster
		// as it has a complete data set and is always the most granular
		result[0] = unaggregated
		completedAttrs = unaggregated.Options().Attributes()
	}

	// Take any partially aggregated namespaces with longer retention or
	// same retention with more granular resolution that may contain
	// a matching metric
	for _, n := range r.partialAggregated {
		if n.Options().Attributes().Retention > completedAttrs.Retention {
			// Higher retention
			result = append(result, n)
			continue
		}
		if n.Options().Attributes().Retention == completedAttrs.Retention &&
			n.Options().Attributes().Resolution < completedAttrs.Resolution {
			// Same retention but more granular resolution
			result = append(result, n)
			continue
		}
	}

	return namespaceCoversPartialQueryRange, result, nil
}

type reusedAggregatedNamespaceSlices struct {
	completeAggregated []ClusterNamespace
	partialAggregated  []ClusterNamespace
}

func (s *m3storage) aggregatedNamespaces(
	slices reusedAggregatedNamespaceSlices,
	filter func(ClusterNamespace) bool,
) reusedAggregatedNamespaceSlices {
	all := s.clusters.ClusterNamespaces()

	// Reset reused slices as necessary
	if slices.completeAggregated == nil {
		slices.completeAggregated = make([]ClusterNamespace, 0, len(all))
	}
	slices.completeAggregated = slices.completeAggregated[:0]
	if slices.partialAggregated == nil {
		slices.partialAggregated = make([]ClusterNamespace, 0, len(all))
	}
	slices.partialAggregated = slices.partialAggregated[:0]

	for _, namespace := range all {
		opts := namespace.Options()
		if opts.Attributes().MetricsType != storage.AggregatedMetricsType {
			// Not an aggregated cluster
			continue
		}

		if filter != nil && !filter(namespace) {
			continue
		}

		downsampleOpts, err := opts.DownsampleOptions()
		if err != nil || !downsampleOpts.All {
			// Cluster does not contain all data, include as part of fan out
			// but separate from
			slices.partialAggregated = append(slices.partialAggregated, namespace)
			continue
		}

		slices.completeAggregated = append(slices.completeAggregated, namespace)
	}

	return slices
}
