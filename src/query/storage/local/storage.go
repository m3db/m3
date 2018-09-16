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

package local

import (
	"context"
	goerrors "errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/execution"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	errNoLocalClustersFulfillsQuery = goerrors.New("no clusters can fulfill query")
)

type queryFanoutType uint

const (
	namespacesCoverAllRetention queryFanoutType = iota
	namespacesCoverPartialRetention
)

type localStorage struct {
	clusters   Clusters
	workerPool pool.ObjectPool
}

// NewStorage creates a new local Storage instance.
func NewStorage(clusters Clusters, workerPool pool.ObjectPool) storage.Storage {
	return &localStorage{clusters: clusters, workerPool: workerPool}
}

// clusterNamespacesForQuery returns the namespaces that need to be
// fanned out to depending on the query time and the namespaces configured.
func (s *localStorage) resolveClusterNamespacesForQuery(
	start time.Time,
	end time.Time,
) (queryFanoutType, ClusterNamespaces, error) {
	now := time.Now()

	unaggregated := s.clusters.UnaggregatedClusterNamespace()
	unaggregatedRetention := unaggregated.Options().Attributes().Retention
	unaggregatedStart := now.Add(-1 * unaggregatedRetention)
	if !unaggregatedStart.After(start) {
		// Highest resolution is unaggregated, return if it can fulfill it
		return namespacesCoverAllRetention, ClusterNamespaces{unaggregated}, nil
	}

	// First determine if any aggregated clusters that can span the whole query
	// range can fulfill the query, if so that's the most optimal strategy, choose
	// the most granular resolution that can and fan out to any partial
	// aggregated namespaces that may holder even more granular resolutions
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

		return namespacesCoverAllRetention, result, nil
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
		return namespacesCoverPartialRetention, result, nil
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

	return namespacesCoverPartialRetention, result, nil
}

type reusedAggregatedNamespaceSlices struct {
	completeAggregated []ClusterNamespace
	partialAggregated  []ClusterNamespace
}

func (s *localStorage) aggregatedNamespaces(
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

func (s *localStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-options.KillChan:
		return nil, errors.ErrQueryInterrupted
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
	fanout, namespaces, err := s.resolveClusterNamespacesForQuery(query.Start, query.End)
	if err != nil {
		return nil, err
	}

	var (
		opts   = storage.FetchOptionsToM3Options(options, query)
		result = multiFetchResult{queryFanoutType: fanout}
		wg     sync.WaitGroup
	)
	for _, namespace := range namespaces {
		namespace := namespace // Capture var

		wg.Add(1)
		go func() {
			r, err := s.fetch(namespace, m3query, opts)
			result.add(namespace.Options().Attributes(), r, err)
			wg.Done()
		}()
	}

	wg.Wait()
	if err := result.err.FinalError(); err != nil {
		return nil, err
	}
	return result.result, nil
}

func (s *localStorage) fetch(
	namespace ClusterNamespace,
	query index.Query,
	opts index.QueryOptions,
) (*storage.FetchResult, error) {
	namespaceID := namespace.NamespaceID()
	session := namespace.Session()

	// TODO (nikunj): Handle second return param
	iters, _, err := session.FetchTagged(namespaceID, query, opts)
	if err != nil {
		return nil, err
	}

	return storage.SeriesIteratorsToFetchResult(iters, namespaceID, s.workerPool)
}

func (s *localStorage) FetchTags(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.SearchResults, error) {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-options.KillChan:
		return nil, errors.ErrQueryInterrupted
	default:
	}

	m3query, err := storage.FetchQueryToM3Query(query)
	if err != nil {
		return nil, err
	}

	// NB(r): Since we don't use a single index we fan out to all clusters.
	// This needs to be optimized, however this is a start.
	namespaces := s.clusters.ClusterNamespaces()

	var (
		opts   = storage.FetchOptionsToM3Options(options, query)
		result multiFetchTagsResult
		wg     sync.WaitGroup
	)
	for _, namespace := range namespaces {
		namespace := namespace // Capture var

		wg.Add(1)
		go func() {
			result.add(s.fetchTags(namespace, m3query, opts))
			wg.Done()
		}()
	}

	wg.Wait()
	if err := result.err.FinalError(); err != nil {
		return nil, err
	}
	return result.result, nil
}

func (s *localStorage) fetchTags(
	namespace ClusterNamespace,
	query index.Query,
	opts index.QueryOptions,
) (*storage.SearchResults, error) {
	namespaceID := namespace.NamespaceID()
	session := namespace.Session()

	// TODO (juchan): Handle second return param
	iter, _, err := session.FetchTaggedIDs(namespaceID, query, opts)
	if err != nil {
		return nil, err
	}

	var metrics models.Metrics
	for iter.Next() {
		m, err := storage.FromM3IdentToMetric(iter.Current())
		if err != nil {
			return nil, err
		}

		metrics = append(metrics, m)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	iter.Finalize()

	return &storage.SearchResults{
		Metrics: metrics,
	}, nil
}

func (s *localStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	// Check if the query was interrupted.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if query == nil {
		return errors.ErrNilWriteQuery
	}

	id := query.Tags.ID()
	common := &writeRequestCommon{
		store:       s,
		annotation:  query.Annotation,
		unit:        query.Unit,
		id:          id,
		tagIterator: storage.TagsToIdentTagIterator(query.Tags),
		attributes:  query.Attributes,
	}

	requests := make([]execution.Request, len(query.Datapoints))
	for idx, datapoint := range query.Datapoints {
		requests[idx] = newWriteRequest(common, datapoint.Timestamp, datapoint.Value)
	}
	return execution.ExecuteParallel(ctx, requests)
}

func (s *localStorage) Type() storage.Type {
	return storage.TypeLocalDC
}

func (s *localStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions) (block.Result, error) {
	fetchResult, err := s.Fetch(ctx, query, options)
	if err != nil {
		return block.Result{}, err
	}

	res, err := storage.FetchResultToBlockResult(fetchResult, query)
	if err != nil {
		return block.Result{}, err
	}

	return res, nil
}

func (s *localStorage) Close() error {
	return nil
}

func (w *writeRequest) Process(ctx context.Context) error {
	common := w.writeRequestCommon
	store := common.store
	id := ident.StringID(common.id)

	var (
		namespace ClusterNamespace
		err       error
	)
	switch common.attributes.MetricsType {
	case storage.UnaggregatedMetricsType:
		namespace = store.clusters.UnaggregatedClusterNamespace()
	case storage.AggregatedMetricsType:
		attrs := RetentionResolution{
			Retention:  common.attributes.Retention,
			Resolution: common.attributes.Resolution,
		}
		var exists bool
		namespace, exists = store.clusters.AggregatedClusterNamespace(attrs)
		if !exists {
			err = fmt.Errorf("no configured cluster namespace for: retention=%s, resolution=%s",
				attrs.Retention.String(), attrs.Resolution.String())
		}
	default:
		metricsType := common.attributes.MetricsType
		err = fmt.Errorf("invalid write request metrics type: %s (%d)",
			metricsType.String(), uint(metricsType))
	}
	if err != nil {
		return err
	}

	namespaceID := namespace.NamespaceID()
	session := namespace.Session()
	return session.WriteTagged(namespaceID, id, common.tagIterator,
		w.timestamp, w.value, common.unit, common.annotation)
}

type writeRequestCommon struct {
	store       *localStorage
	annotation  []byte
	unit        xtime.Unit
	id          string
	tagIterator ident.TagIterator
	attributes  storage.Attributes
}

type writeRequest struct {
	writeRequestCommon *writeRequestCommon
	timestamp          time.Time
	value              float64
}

func newWriteRequest(writeRequestCommon *writeRequestCommon, timestamp time.Time, value float64) execution.Request {
	return &writeRequest{
		writeRequestCommon: writeRequestCommon,
		timestamp:          timestamp,
		value:              value,
	}
}

type multiFetchResult struct {
	sync.Mutex
	result           *storage.FetchResult
	err              xerrors.MultiError
	dedupeFirstAttrs storage.Attributes
	dedupeMap        map[string]multiFetchResultSeries
	queryFanoutType  queryFanoutType
}

type multiFetchResultSeries struct {
	idx   int
	attrs storage.Attributes
}

func (r *multiFetchResult) add(
	attrs storage.Attributes,
	result *storage.FetchResult,
	err error,
) {
	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.err = r.err.Add(err)
		return
	}

	if r.result == nil {
		r.result = result
		r.dedupeFirstAttrs = attrs
		return
	}

	r.result.HasNext = r.result.HasNext && result.HasNext
	r.result.LocalOnly = r.result.LocalOnly && result.LocalOnly

	// Need to dedupe
	if r.dedupeMap == nil {
		r.dedupeMap = make(map[string]multiFetchResultSeries, len(r.result.SeriesList))
		for idx, s := range r.result.SeriesList {
			r.dedupeMap[s.Name()] = multiFetchResultSeries{
				idx:   idx,
				attrs: r.dedupeFirstAttrs,
			}
		}
	}

	for _, s := range result.SeriesList {
		id := s.Name()

		existing, exists := r.dedupeMap[id]
		if exists {
			var existsBetter bool
			switch r.queryFanoutType {
			case namespacesCoverAllRetention:
				// Already exists and resolution of result we are adding is not as precise
				existsBetter = existing.attrs.Resolution <= attrs.Resolution
			case namespacesCoverPartialRetention:
				// Already exists and either has longer retention, or the same retention
				// and result we are adding is not as precise
				longerRetention := existing.attrs.Retention > attrs.Retention
				sameRetentionAndSameOrMoreGranularResolution :=
					existing.attrs.Retention == attrs.Retention &&
						existing.attrs.Resolution <= attrs.Resolution
				existsBetter = longerRetention || sameRetentionAndSameOrMoreGranularResolution
			default:
				panic(fmt.Sprintf("unknown query fanout type: %d", r.queryFanoutType))
			}
			if existsBetter {
				// Existing result is better
				continue
			}
		}

		// Does not exist already or more precise, add result
		var idx int
		if !exists {
			idx = len(r.result.SeriesList)
			r.result.SeriesList = append(r.result.SeriesList, s)
		} else {
			idx = existing.idx
			r.result.SeriesList[idx] = s
		}

		r.dedupeMap[id] = multiFetchResultSeries{
			idx:   idx,
			attrs: attrs,
		}
	}
}

type multiFetchTagsResult struct {
	sync.Mutex
	result    *storage.SearchResults
	err       xerrors.MultiError
	dedupeMap map[string]struct{}
}

func (r *multiFetchTagsResult) add(
	result *storage.SearchResults,
	err error,
) {
	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.err = r.err.Add(err)
		return
	}

	if r.result == nil {
		r.result = result
		return
	}

	// Need to dedupe
	if r.dedupeMap == nil {
		r.dedupeMap = make(map[string]struct{}, len(r.result.Metrics))
		for _, s := range r.result.Metrics {
			r.dedupeMap[s.ID] = struct{}{}
		}
	}

	for _, s := range result.Metrics {
		id := s.ID
		_, exists := r.dedupeMap[id]
		if exists {
			// Already exists
			continue
		}

		// Does not exist already, add result
		r.result.Metrics = append(r.result.Metrics, s)
		r.dedupeMap[id] = struct{}{}
	}
}
