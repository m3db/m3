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

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/execution"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

const (
	initRawFetchAllocSize = 32
)

type localStorage struct {
	session       client.Session
	namespace     ident.ID
	millisPerStep int64
}

// NewStorage creates a new local Storage instance.
func NewStorage(session client.Session, namespace string, resolution time.Duration) storage.Storage {
	return &localStorage{session: session, namespace: ident.StringID(namespace), millisPerStep: stepFromResolution(resolution)}
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

		result := make([]ts.Datapoint, 0, initRawFetchAllocSize)
		for iter.Next() {
			dp, _, _ := iter.Current()
			result = append(result, ts.Datapoint{Timestamp: dp.Timestamp, Value: dp.Value})
		}

		values := ts.NewValues(ctx, int(s.millisPerStep), len(result))

		// TODO (nikunj): Figure out consolidation here
		for i, v := range result {
			values.SetValueAt(i, v.Value)
		}

		series := ts.NewSeries(ctx, metric.ID, query.Start, values, metric.Tags)
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

	results, err := s.session.FetchTaggedIDs(s.namespace, m3query, opts)
	if err != nil {
		return nil, err
	}

	iter := results.Iterator

	var metrics models.Metrics
	for iter.Next() {
		m, err := storage.FromM3IdentToMetric(results.Iterator.Current())
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
		store:      s,
		annotation: query.Annotation,
		unit:       query.Unit,
		id:         id,
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

func (w *writeRequest) Process(ctx context.Context) error {
	common := w.writeRequestCommon
	store := common.store
	id := ident.StringID(common.id)
	return store.session.Write(store.namespace, id, w.timestamp, w.value, common.unit, common.annotation)
}

type writeRequestCommon struct {
	store      *localStorage
	annotation []byte
	unit       xtime.Unit
	id         string
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

func stepFromResolution(resolution time.Duration) int64 {
	return xtime.ToNormalizedDuration(resolution, time.Millisecond)
}
