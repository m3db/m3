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
	"time"

	"github.com/m3db/m3db/src/coordinator/errors"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/ts"
	"github.com/m3db/m3db/src/coordinator/util/execution"
	"github.com/m3db/m3db/src/dbnode/client"

	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

const (
	initRawFetchAllocSize = 32
)

type localStorage struct {
	session   client.Session
	namespace ident.ID
}

// NewStorage creates a new local Storage instance.
func NewStorage(session client.Session, namespace string) storage.Storage {
	return &localStorage{session: session, namespace: ident.StringID(namespace)}
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

	opts := storage.FetchOptionsToM3Options(options, query)
	// TODO (nikunj): Handle second return param
	iters, _, err := s.session.FetchTagged(s.namespace, m3query, opts)
	if err != nil {
		return nil, err
	}

	defer iters.Close()

	seriesList := make([]*ts.Series, iters.Len())
	for i, iter := range iters.Iters() {
		metric, err := storage.FromM3IdentToMetric(s.namespace, iter.ID(), iter.Tags())
		if err != nil {
			return nil, err
		}

		datapoints := make(ts.Datapoints, 0, initRawFetchAllocSize)
		for iter.Next() {
			dp, _, _ := iter.Current()
			datapoints = append(datapoints, ts.Datapoint{Timestamp: dp.Timestamp, Value: dp.Value})
		}

		series := ts.NewSeries(metric.ID, datapoints, metric.Tags)
		seriesList[i] = series
	}

	return &storage.FetchResult{
		SeriesList: seriesList,
	}, nil
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

	opts := storage.FetchOptionsToM3Options(options, query)
	// TODO (juchan): Handle second return param
	iter, _, err := s.session.FetchTaggedIDs(s.namespace, m3query, opts)
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
	ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (storage.BlockResult, error) {
	return storage.BlockResult{}, errors.ErrNotImplemented
}

func (w *writeRequest) Process(ctx context.Context) error {
	common := w.writeRequestCommon
	store := common.store
	id := ident.StringID(common.id)
	return store.session.WriteTagged(store.namespace, id, common.tagIterator, w.timestamp, w.value, common.unit, common.annotation)
}

type writeRequestCommon struct {
	store       *localStorage
	annotation  []byte
	unit        xtime.Unit
	id          string
	tagIterator ident.TagIterator
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

func (s *localStorage) Close() error {
	s.namespace.Finalize()
	return nil
}
