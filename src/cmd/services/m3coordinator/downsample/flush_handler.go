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

package downsample

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/convert"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	// MetricsOptionIDSchemeTagName is a meta tag
	// that describes the ID should use a specific ID scheme.
	MetricsOptionIDSchemeTagName = []byte("__option_id_scheme__")
	// GraphiteIDSchemeTagValue specifies that the graphite ID
	// scheme should be used for a metric.
	GraphiteIDSchemeTagValue = []byte("graphite")
)

var (
	aggregationSuffixTag = []byte("agg")
)

type downsamplerFlushHandler struct {
	sync.RWMutex
	storage                storage.Storage
	metricTagsIteratorPool serialize.MetricTagsIteratorPool
	workerPool             xsync.WorkerPool
	instrumentOpts         instrument.Options
	metrics                downsamplerFlushHandlerMetrics
	tagOptions             models.TagOptions
}

type downsamplerFlushHandlerMetrics struct {
	flushSuccess tally.Counter
	flushErrors  tally.Counter
}

func newDownsamplerFlushHandlerMetrics(
	scope tally.Scope,
) downsamplerFlushHandlerMetrics {
	return downsamplerFlushHandlerMetrics{
		flushSuccess: scope.Counter("flush-success"),
		flushErrors:  scope.Counter("flush-errors"),
	}
}

func newDownsamplerFlushHandler(
	storage storage.Storage,
	metricTagsIteratorPool serialize.MetricTagsIteratorPool,
	workerPool xsync.WorkerPool,
	tagOptions models.TagOptions,
	instrumentOpts instrument.Options,
) handler.Handler {
	scope := instrumentOpts.MetricsScope().SubScope("downsampler-flush-handler")
	return &downsamplerFlushHandler{
		storage:                storage,
		metricTagsIteratorPool: metricTagsIteratorPool,
		workerPool:             workerPool,
		instrumentOpts:         instrumentOpts,
		metrics:                newDownsamplerFlushHandlerMetrics(scope),
		tagOptions:             tagOptions,
	}
}

func (h *downsamplerFlushHandler) NewWriter(
	scope tally.Scope,
) (writer.Writer, error) {
	return &downsamplerFlushHandlerWriter{
		tagOptions: h.tagOptions,
		ctx:        context.Background(),
		handler:    h,
	}, nil
}

func (h *downsamplerFlushHandler) Close() {
}

type downsamplerFlushHandlerWriter struct {
	tagOptions models.TagOptions
	wg         sync.WaitGroup
	ctx        context.Context
	handler    *downsamplerFlushHandler
}

func (w *downsamplerFlushHandlerWriter) Write(
	mp aggregated.ChunkedMetricWithStoragePolicy,
) error {
	w.wg.Add(1)
	w.handler.workerPool.Go(func() {
		defer w.wg.Done()

		logger := w.handler.instrumentOpts.Logger()

		iter := w.handler.metricTagsIteratorPool.Get()
		iter.Reset(mp.ChunkedID.Data)

		expected := iter.NumTags()
		chunkSuffix := mp.ChunkedID.Suffix
		if len(chunkSuffix) != 0 {
			expected++
		}

		tags := models.NewTags(expected, w.tagOptions)
		for iter.Next() {
			name, value := iter.Current()

			// NB(r): Quite gross, need to actually make it possible to plumb this
			// through for each metric.
			// TODO_FIX_GRAPHITE_TAGGING: Using this string constant to track
			// all places worth fixing this hack. There is at least one
			// other path where flows back to the coordinator from the aggregator
			// and this tag is interpreted, eventually need to handle more cleanly.
			if bytes.Equal(name, MetricsOptionIDSchemeTagName) {
				if bytes.Equal(value, GraphiteIDSchemeTagValue) &&
					tags.Opts.IDSchemeType() != models.TypeGraphite {
					iter.Reset(mp.ChunkedID.Data)
					tags.Opts = w.tagOptions.SetIDSchemeType(models.TypeGraphite)
					tags.Tags = tags.Tags[:0]
				}
				// Continue, whether we updated and need to restart iteration,
				// or if passing for the second time
				continue
			}

			tags = tags.AddTag(models.Tag{Name: name, Value: value}.Clone())
		}

		if len(chunkSuffix) != 0 {
			tags = tags.AddTag(models.Tag{Name: aggregationSuffixTag, Value: chunkSuffix}.Clone())
		}

		err := iter.Err()
		iter.Close()
		if err != nil {
			logger.Error("downsampler flush error preparing write", zap.Error(err))
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		writeQuery, err := storage.NewWriteQuery(storage.WriteQueryOptions{
			Tags: tags,
			Datapoints: ts.Datapoints{ts.Datapoint{
				Timestamp: time.Unix(0, mp.TimeNanos),
				Value:     mp.Value,
			}},
			Unit:       convert.UnitForM3DB(mp.StoragePolicy.Resolution().Precision),
			Annotation: mp.Annotation,
			Attributes: storagemetadata.Attributes{
				MetricsType: storagemetadata.AggregatedMetricsType,
				Retention:   mp.StoragePolicy.Retention().Duration(),
				Resolution:  mp.StoragePolicy.Resolution().Window,
			},
		})
		if err != nil {
			logger.Error("downsampler flush error creating write query", zap.Error(err))
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		if err := w.handler.storage.Write(w.ctx, writeQuery); err != nil {
			logger.Error("downsampler flush error failed write", zap.Error(err))
			w.handler.metrics.flushErrors.Inc(1)
			return
		}

		w.handler.metrics.flushSuccess.Inc(1)
	})

	return nil
}

func (w *downsamplerFlushHandlerWriter) Flush() error {
	// NB(r): This is just simply waiting for inflight requests
	// to complete since this flush handler isn't connection based.
	w.wg.Wait()
	return nil
}

func (w *downsamplerFlushHandlerWriter) Close() error {
	// NB(r): This is a no-op since this flush handler isn't connection based.
	return nil
}
