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

package ingestm3msg

import (
	"bytes"
	"context"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/convert"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/sampler"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Options configures the ingester.
type Options struct {
	Appender          storage.Appender
	Workers           xsync.PooledWorkerPool
	PoolOptions       pool.ObjectPoolOptions
	TagDecoderPool    serialize.TagDecoderPool
	RetryOptions      retry.Options
	Sampler           *sampler.Sampler
	InstrumentOptions instrument.Options
	TagOptions        models.TagOptions
	StoreMetricsType  bool
}

type ingestMetrics struct {
	ingestInternalError     tally.Counter
	ingestNonRetryableError tally.Counter
	ingestSuccess           tally.Counter
}

func newIngestMetrics(scope tally.Scope) ingestMetrics {
	return ingestMetrics{
		ingestInternalError: scope.Tagged(map[string]string{
			"error_type": "internal_error",
		}).Counter("ingest-error"),
		ingestNonRetryableError: scope.Tagged(map[string]string{
			"error_type": "non_retryable_error",
		}).Counter("ingest-error"),
		ingestSuccess: scope.Counter("ingest-success"),
	}
}

// Ingester ingests metrics with a worker pool.
type Ingester struct {
	workers xsync.PooledWorkerPool
	p       pool.ObjectPool
}

// NewIngester creates an ingester.
func NewIngester(
	opts Options,
) *Ingester {
	retrier := retry.NewRetrier(opts.RetryOptions)
	m := newIngestMetrics(opts.InstrumentOptions.MetricsScope())
	p := pool.NewObjectPool(opts.PoolOptions)
	tagOpts := opts.TagOptions
	if tagOpts == nil {
		tagOpts = models.NewTagOptions()
	}
	p.Init(
		func() interface{} {
			// NB: we don't need a pool for the tag decoder since the ops are
			// pooled, but currently this is the only way to get tag decoder.
			tagDecoder := opts.TagDecoderPool.Get()
			op := ingestOp{
				storageAppender:  opts.Appender,
				retrier:          retrier,
				iter:             serialize.NewMetricTagsIterator(tagDecoder, nil),
				tagOpts:          tagOpts,
				pool:             p,
				metrics:          m,
				logger:           opts.InstrumentOptions.Logger(),
				sampler:          opts.Sampler,
				storeMetricsType: opts.StoreMetricsType,
			}
			op.attemptFn = op.attempt
			op.ingestFn = op.ingest
			return &op
		},
	)
	return &Ingester{
		workers: opts.Workers,
		p:       p,
	}
}

// Ingest ingests a metric asynchronously with callback.
func (i *Ingester) Ingest(
	ctx context.Context,
	id []byte,
	metricType ts.PromMetricType,
	metricNanos, encodeNanos int64,
	value float64,
	sp policy.StoragePolicy,
	callback m3msg.Callbackable,
) {
	op := i.p.Get().(*ingestOp)
	op.ctx = ctx
	op.id = id
	op.metricType = metricType
	op.metricNanos = metricNanos
	op.value = value
	op.sp = sp
	op.callback = callback
	i.workers.Go(op.ingestFn)
}

type ingestOp struct {
	storageAppender  storage.Appender
	retrier          retry.Retrier
	iter             id.SortedTagIterator
	tagOpts          models.TagOptions
	pool             pool.ObjectPool
	metrics          ingestMetrics
	logger           *zap.Logger
	sampler          *sampler.Sampler
	attemptFn        retry.Fn
	ingestFn         func()
	storeMetricsType bool

	ctx         context.Context
	id          []byte
	metricType  ts.PromMetricType
	metricNanos int64
	value       float64
	sp          policy.StoragePolicy
	callback    m3msg.Callbackable
	tags        models.Tags
	datapoints  ts.Datapoints
	writeQuery  storage.WriteQuery
}

func (op *ingestOp) sample() bool {
	if op.sampler == nil {
		return false
	}
	return op.sampler.Sample()
}

func (op *ingestOp) ingest() {
	if err := op.resetWriteQuery(); err != nil {
		op.metrics.ingestInternalError.Inc(1)
		op.callback.Callback(m3msg.OnRetriableError)
		op.pool.Put(op)
		if op.sample() {
			op.logger.Error("could not reset ingest op", zap.Error(err))
		}
		return
	}
	if err := op.retrier.Attempt(op.attemptFn); err != nil {
		nonRetryableErr := xerrors.IsNonRetryableError(err)
		if nonRetryableErr {
			op.callback.Callback(m3msg.OnNonRetriableError)
			op.metrics.ingestNonRetryableError.Inc(1)
		} else {
			op.callback.Callback(m3msg.OnRetriableError)
			op.metrics.ingestInternalError.Inc(1)
		}

		// NB(r): Always log non-retriable errors since they are usually
		// a very small minority and when they go missing it can be frustrating
		// not being able to find them (usually bad request errors).
		if nonRetryableErr || op.sample() {
			op.logger.Error("could not write ingest op",
				zap.Error(err),
				zap.Bool("retryableError", !nonRetryableErr))
		}

		op.pool.Put(op)
		return
	}
	op.metrics.ingestSuccess.Inc(1)
	op.callback.Callback(m3msg.OnSuccess)
	op.pool.Put(op)
}

func (op *ingestOp) attempt() error {
	return op.storageAppender.Write(op.ctx, &op.writeQuery)
}

func (op *ingestOp) resetWriteQuery() error {
	if err := op.resetTags(); err != nil {
		return err
	}
	op.resetDataPoints()

	wq := storage.WriteQueryOptions{
		Tags:       op.tags,
		Datapoints: op.datapoints,
		Unit:       convert.UnitForM3DB(op.sp.Resolution().Precision),
		Attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Resolution:  op.sp.Resolution().Window,
			Retention:   op.sp.Retention().Duration(),
		},
	}

	if op.storeMetricsType {
		var err error
		wq.Annotation, err = op.convertTypeToAnnotation(op.metricType)
		if err != nil {
			return err
		}
	}

	return op.writeQuery.Reset(wq)
}

func (op *ingestOp) convertTypeToAnnotation(tp ts.PromMetricType) ([]byte, error) {
	if tp == ts.PromMetricTypeUnknown {
		return nil, nil
	}

	handleValueResets := false
	if tp == ts.PromMetricTypeCounter {
		handleValueResets = true
	}

	annotationPayload, err := storage.SeriesAttributesToAnnotationPayload(tp, handleValueResets)
	if err != nil {
		return nil, err
	}
	annot, err := annotationPayload.Marshal()
	if err != nil {
		return nil, err
	}

	if len(annot) == 0 {
		annot = nil
	}
	return annot, nil
}

func (op *ingestOp) resetTags() error {
	op.iter.Reset(op.id)
	op.tags.Tags = op.tags.Tags[:0]
	op.tags.Opts = op.tagOpts
	for op.iter.Next() {
		name, value := op.iter.Current()

		// TODO_FIX_GRAPHITE_TAGGING: Using this string constant to track
		// all places worth fixing this hack. There is at least one
		// other path where flows back to the coordinator from the aggregator
		// and this tag is interpreted, eventually need to handle more cleanly.
		if bytes.Equal(name, downsample.MetricsOptionIDSchemeTagName) {
			if bytes.Equal(value, downsample.GraphiteIDSchemeTagValue) &&
				op.tags.Opts.IDSchemeType() != models.TypeGraphite {
				// Restart iteration with graphite tag options parsing
				op.iter.Reset(op.id)
				op.tags.Tags = op.tags.Tags[:0]
				op.tags.Opts = op.tags.Opts.SetIDSchemeType(models.TypeGraphite)
			}
			// Continue, whether we updated and need to restart iteration,
			// or if passing for the second time
			continue
		}

		op.tags = op.tags.AddTagWithoutNormalizing(models.Tag{
			Name:  name,
			Value: value,
		}.Clone())
	}
	op.tags.Normalize()
	return op.iter.Err()
}

func (op *ingestOp) resetDataPoints() {
	if len(op.datapoints) != 1 {
		op.datapoints = make(ts.Datapoints, 1)
	}
	op.datapoints[0].Timestamp = time.Unix(0, op.metricNanos)
	op.datapoints[0].Value = op.value
}
