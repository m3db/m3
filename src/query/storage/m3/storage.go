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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/execution"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var (
	errNoLocalClustersFulfillsQuery = goerrors.New("no clusters can fulfill query")
)

type m3storage struct {
	clusters   Clusters
	workerPool pool.ObjectPool
}

// NewStorage creates a new local m3storage instance.
func NewStorage(clusters Clusters, workerPool pool.ObjectPool) Storage {
	return &m3storage{clusters: clusters, workerPool: workerPool}
}

func (s *m3storage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	raw, cleanup, err := s.FetchRaw(ctx, query, options)
	defer cleanup()
	if err != nil {
		return nil, err
	}

	return storage.SeriesIteratorsToFetchResult(raw, nil, s.workerPool)
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

func (s *m3storage) FetchRaw(
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

		clusterStart := now.Add(-1 * namespace.Options().Attributes().Retention)

		// Only include if cluster can completely fulfill the range
		if clusterStart.After(query.Start) {
			continue
		}

		fetches++

		wg.Add(1)
		go func() {
			session := namespace.Session()
			ns := namespace.NamespaceID()
			iters, _, err := session.FetchTagged(ns, m3query, opts)
			result.add(namespace.Options().Attributes(), iters, err)
			wg.Done()
		}()
	}
	if fetches == 0 {
		return nil, noop, errNoLocalClustersFulfillsQuery
	}
	wg.Wait()
	if err := result.err.FinalError(); err != nil {
		return nil, noop, err
	}

	return result.iterators, result.cleanup, nil
}

func (s *m3storage) FetchTags(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
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

		clusterStart := now.Add(-1 * namespace.Options().Attributes().Retention)

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

func (s *m3storage) fetchTags(
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

func (s *m3storage) Type() storage.Type {
	return storage.TypeLocalDC
}

func (s *m3storage) Close() error {
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
	store       *m3storage
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

func newWriteRequest(
	writeRequestCommon *writeRequestCommon,
	timestamp time.Time,
	value float64,
) execution.Request {
	return &writeRequest{
		writeRequestCommon: writeRequestCommon,
		timestamp:          timestamp,
		value:              value,
	}
}
