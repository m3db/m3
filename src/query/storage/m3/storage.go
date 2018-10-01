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
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
)

var (
	errNoLocalClustersFulfillsQuery = goerrors.New("no clusters can fulfill query")
)

type m3storage struct {
	clusters        Clusters
	readWorkerPool  pool.ObjectPool
	writeWorkerPool xsync.PooledWorkerPool
}

// NewStorage creates a new local m3storage instance.
// TODO: Consider combining readWorkerPool and writeWorkerPool
func NewStorage(clusters Clusters, workerPool pool.ObjectPool, writeWorkerPool xsync.PooledWorkerPool) Storage {
	return &m3storage{clusters: clusters, readWorkerPool: workerPool, writeWorkerPool: writeWorkerPool}
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

	return storage.SeriesIteratorsToFetchResult(raw, s.readWorkerPool, false)
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
		wg         sync.WaitGroup
	)
	if len(namespaces) == 0 {
		return nil, noop, fmt.Errorf("no namespaces configured")
	}

	pools, err := namespaces[0].Session().IteratorPools()
	if err != nil {
		return nil, noop, fmt.Errorf("unable to retrieve iterator pools: %v", err)
	}

	result := newMultiFetchResult(pools)

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
			// Ignore error from getting iterator pools, since operation
			// will not be dramatically impacted if pools is nil
			result.Add(namespace.Options().Attributes(), iters, err)
			wg.Done()
		}()
	}

	if fetches == 0 {
		return nil, noop, errNoLocalClustersFulfillsQuery
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
		_, id, it := iter.Current()
		m, err := storage.FromM3IdentToMetric(id, it)
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

	// TODO: Consider caching id -> identID (requires
	// setting NoFinalize().
	var (
		// TODO: Pool this once an ident pool is setup
		buf   = make([]byte, 0, query.Tags.IDLen())
		idBuf = query.Tags.WriteBytesID(buf)
		id    = ident.BytesID(idBuf)
	)
	// Set id to NoFinalize to avoid cloning it in write operations
	id.NoFinalize()
	tagIterator := storage.TagsToIdentTagIterator(query.Tags)

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
