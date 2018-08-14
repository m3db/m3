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

type localStorage struct {
	clusters   Clusters
	workerPool pool.ObjectPool
}

// NewStorage creates a new local Storage instance.
func NewStorage(clusters Clusters, workerPool pool.ObjectPool) storage.Storage {
	return &localStorage{clusters: clusters, workerPool: workerPool}
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
	var (
		opts       = storage.FetchOptionsToM3Options(options, query)
		namespaces = s.clusters.ClusterNamespaces()
		now        = time.Now()
		fetches    = 0
		result     multiFetchResult
		wg         sync.WaitGroup
	)
	for _, namespace := range namespaces {
		namespace := namespace // Capture var

		clusterStart := now.Add(-1 * namespace.Attributes().Retention)

		// Only include if cluster can completely fulfill the range
		if clusterStart.After(query.Start) {
			continue
		}

		fetches++

		wg.Add(1)
		go func() {
			r, err := s.fetch(namespace, m3query, opts)
			result.add(namespace.Attributes(), r, err)
			wg.Done()
		}()
	}

	if fetches == 0 {
		return nil, errNoLocalClustersFulfillsQuery
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

	var (
		opts       = storage.FetchOptionsToM3Options(options, query)
		namespaces = s.clusters.ClusterNamespaces()
		now        = time.Now()
		fetches    = 0
		result     multiFetchTagsResult
		wg         sync.WaitGroup
	)
	for _, namespace := range namespaces {
		namespace := namespace // Capture var

		clusterStart := now.Add(-1 * namespace.Attributes().Retention)

		// Only include if cluster can completely fulfill the range
		if clusterStart.After(query.Start) {
			continue
		}

		fetches++

		wg.Add(1)
		go func() {
			result.add(s.fetchTags(namespace, m3query, opts))
			wg.Done()
		}()
	}

	if fetches == 0 {
		return nil, errNoLocalClustersFulfillsQuery
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
		if exists && existing.attrs.Resolution <= attrs.Resolution {
			// Already exists and resolution of result we are adding is not as precise
			continue
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
