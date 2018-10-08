package ingest

import (
	"context"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/server/m3msg"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/retry"
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
	opts *Options,
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
				s:  opts.Appender,
				r:  retrier,
				it: serialize.NewMetricTagsIterator(tagDecoder, nil),
				p:  p,
				m:  m,
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
	id []byte,
	metricTimeNanos int64,
	value float64,
	sp policy.StoragePolicy,
	callback *m3msg.RefCountedCallback,
) {
	op := i.p.Get().(*ingestOp)
	op.id = id
	op.metricTimeNanos = metricTimeNanos
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
	attemptFn retry.Fn
	ingestFn  func()

	id              []byte
	metricTimeNanos int64
	value           float64
	sp              policy.StoragePolicy
	callback        *m3msg.RefCountedCallback
	q               storage.WriteQuery
}

func (op *ingestOp) ingest() {
	if err := op.resetWriteQuery(); err != nil {
		op.m.ingestError.Inc(1)
		op.callback.Callback(m3msg.OnRetriableError)
		op.p.Put(op)
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
		return
	}
	op.m.ingestSuccess.Inc(1)
	op.callback.Callback(m3msg.OnSuccess)
	op.p.Put(op)
}

func (op *ingestOp) attempt() error {
	return op.s.Write(
		// NB: No timeout is needed for this as the m3db client has a timeout
		// configured to it.
		context.Background(),
		&op.q,
	)
}

func (op *ingestOp) resetWriteQuery() error {
	if err := op.resetTags(); err != nil {
		return err
	}
	op.resetDataPoints()
	op.q.Raw = string(op.id)
	op.q.Unit = op.sp.Resolution().Precision
	op.q.Attributes.MetricsType = storage.AggregatedMetricsType
	op.q.Attributes.Resolution = op.sp.Resolution().Window
	op.q.Attributes.Retention = op.sp.Retention().Duration()
	return nil
}

func (op *ingestOp) resetTags() error {
	op.it.Reset(op.id)
	op.q.Tags = op.q.Tags[:0]
	for op.it.Next() {
		name, value := op.it.Current()
		op.q.Tags = append(op.q.Tags, models.Tag{Name: name, Value: value})
	}
	return op.it.Err()
}

func (op *ingestOp) resetDataPoints() {
	if len(op.q.Datapoints) != 1 {
		op.q.Datapoints = make(ts.Datapoints, 1)
	}
	op.q.Datapoints[0].Timestamp = time.Unix(0, op.metricTimeNanos)
	op.q.Datapoints[0].Value = op.value
}
