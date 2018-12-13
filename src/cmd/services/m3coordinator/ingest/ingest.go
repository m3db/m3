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

package ingest

import (
	"context"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/convert"
	"github.com/m3db/m3/src/x/serialize"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/sampler"
	xsync "github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
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
}

type ingestMetrics struct {
	ingestError   tally.Counter
	ingestSuccess tally.Counter
}

func newIngestMetrics(scope tally.Scope) ingestMetrics {
	return ingestMetrics{
		ingestError:   scope.Counter("ingest-error"),
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
	p.Init(
		func() interface{} {
			// NB: we don't need a pool for the tag decoder since the ops are
			// pooled, but currently this is the only way to get tag decoder.
			tagDecoder := opts.TagDecoderPool.Get()
			op := ingestOp{
				s:       opts.Appender,
				r:       retrier,
				it:      serialize.NewMetricTagsIterator(tagDecoder, nil),
				p:       p,
				m:       m,
				logger:  opts.InstrumentOptions.Logger(),
				sampler: opts.Sampler,
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
	metricTime time.Time,
	value float64,
	sp policy.StoragePolicy,
	callback *m3msg.RefCountedCallback,
) {
	op := i.p.Get().(*ingestOp)
	op.c = ctx
	op.id = id
	op.metricTime = metricTime
	op.value = value
	op.sp = sp
	op.callback = callback
	i.workers.Go(op.ingestFn)
}

type ingestOp struct {
	s         storage.Appender
	r         retry.Retrier
	it        id.SortedTagIterator
	p         pool.ObjectPool
	m         ingestMetrics
	logger    log.Logger
	sampler   *sampler.Sampler
	attemptFn retry.Fn
	ingestFn  func()

	c          context.Context
	id         []byte
	metricTime time.Time
	value      float64
	sp         policy.StoragePolicy
	callback   *m3msg.RefCountedCallback
	q          storage.WriteQuery
}

func (op *ingestOp) ingest() {
	if err := op.resetWriteQuery(); err != nil {
		op.m.ingestError.Inc(1)
		op.callback.Callback(m3msg.OnRetriableError)
		op.p.Put(op)
		if op.sampler.Sample() {
			op.logger.Errorf("could not reset ingest op: %v", err)
		}
		return
	}
	if err := op.r.Attempt(op.attemptFn); err != nil {
		if xerrors.IsNonRetryableError(err) {
			op.callback.Callback(m3msg.OnNonRetriableError)
		} else {
			op.callback.Callback(m3msg.OnRetriableError)
		}
		op.m.ingestError.Inc(1)
		op.p.Put(op)
		if op.sampler.Sample() {
			op.logger.Errorf("could not write ingest op: %v", err)
		}
		return
	}
	op.m.ingestSuccess.Inc(1)
	op.callback.Callback(m3msg.OnSuccess)
	op.p.Put(op)
}

func (op *ingestOp) attempt() error {
	return op.s.Write(op.c, &op.q)
}

func (op *ingestOp) resetWriteQuery() error {
	if err := op.resetTags(); err != nil {
		return err
	}
	op.resetDataPoints()
	op.q.Unit = convert.UnitForM3DB(op.sp.Resolution().Precision)
	op.q.Attributes.MetricsType = storage.AggregatedMetricsType
	op.q.Attributes.Resolution = op.sp.Resolution().Window
	op.q.Attributes.Retention = op.sp.Retention().Duration()
	return nil
}

func (op *ingestOp) resetTags() error {
	op.it.Reset(op.id)
	op.q.Tags.Tags = op.q.Tags.Tags[:0]
	for op.it.Next() {
		name, value := op.it.Current()
		op.q.Tags = op.q.Tags.AddTag(models.Tag{
			Name:  name,
			Value: value,
		}.Clone())
	}
	return op.it.Err()
}

func (op *ingestOp) resetDataPoints() {
	if len(op.q.Datapoints) != 1 {
		op.q.Datapoints = make(ts.Datapoints, 1)
	}
	op.q.Datapoints[0].Timestamp = op.metricTime
	op.q.Datapoints[0].Value = op.value
}
