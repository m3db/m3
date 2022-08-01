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

package client

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	errInstanceWriterClosed    = errors.New("instance writer is closed")
	errUnrecognizedMetricType  = errors.New("unrecognized metric type")
	errUnrecognizedPayloadType = errors.New("unrecognized payload type")
	errFlushInProgress         = errors.New("flush is in progress")
)

type instanceWriter interface {
	// Write writes a metric payload for a given shard.
	Write(shard uint32, payload payloadUnion) error

	// Flush flushes any buffered metrics.
	Flush() error

	// QueueSize returns the size of the instance queue.
	QueueSize() int

	// Close closes the writer.
	Close() error
}

type newLockedEncoderFn func(protobuf.UnaggregatedOptions) *lockedEncoder

type writer struct {
	metrics            writerMetrics
	encoderOpts        protobuf.UnaggregatedOptions
	queue              instanceQueue
	log                *zap.Logger
	encodersByShard    map[uint32]*lockedEncoder
	newLockedEncoderFn newLockedEncoderFn
	maxTimerBatchSize  int
	maxBatchSize       int
	encoderByShardMtx  sync.RWMutex
	closed             atomic.Bool
	isFlushActive      atomic.Bool
}

func newInstanceWriter(instance placement.Instance, opts Options) instanceWriter {
	var (
		iOpts     = opts.InstrumentOptions()
		scope     = iOpts.MetricsScope()
		queueOpts = opts.SetInstrumentOptions(iOpts.SetMetricsScope(scope.SubScope("queue")))
	)
	w := &writer{
		log:               iOpts.Logger(),
		metrics:           newWriterMetrics(scope),
		maxBatchSize:      opts.MaxBatchSize(),
		maxTimerBatchSize: opts.MaxTimerBatchSize(),
		encoderOpts:       opts.EncoderOptions(),
		queue:             newInstanceQueue(instance, queueOpts),
		encodersByShard:   make(map[uint32]*lockedEncoder),
	}

	w.isFlushActive.Store(false)
	w.newLockedEncoderFn = newLockedEncoder
	return w
}

// Write takes the payload and writes it to the per-shard encoder.
func (w *writer) Write(shard uint32, payload payloadUnion) error {
	w.encoderByShardMtx.RLock()
	if w.closed.Load() {
		w.encoderByShardMtx.RUnlock()
		return errInstanceWriterClosed
	}
	encoder, exists := w.encodersByShard[shard]
	if exists {
		err := w.encodeWithLock(encoder, payload)
		w.encoderByShardMtx.RUnlock()
		return err
	}
	w.encoderByShardMtx.RUnlock()

	w.encoderByShardMtx.Lock()
	if w.closed.Load() {
		w.encoderByShardMtx.Unlock()
		return errInstanceWriterClosed
	}
	encoder, exists = w.encodersByShard[shard]
	if exists {
		err := w.encodeWithLock(encoder, payload)
		w.encoderByShardMtx.Unlock()
		return err
	}
	encoder = w.newLockedEncoderFn(w.encoderOpts)
	w.encodersByShard[shard] = encoder
	err := w.encodeWithLock(encoder, payload)
	w.encoderByShardMtx.Unlock()

	return err
}

// Flush loops through all encoders in encodersByShard, grabs the encoded
// payload and writes it to the queue. From there the queue is drained
// towards the destination via the transport in a blocking manner
func (w *writer) Flush() error {
	if w.closed.Load() {
		return errInstanceWriterClosed
	}

	if !w.isFlushActive.CAS(false, true) {
		// Flush is already active, bail
		w.metrics.skippedFlushes.Inc(1)
		return errFlushInProgress
	}

	defer w.isFlushActive.Store(false)

	err := w.flush()

	if err != nil {
		w.metrics.flushErrors.Inc(1)
		return err
	}
	return nil
}

func (w *writer) Close() error {
	if !w.closed.CAS(false, true) {
		return errInstanceWriterClosed
	}

	if w.isFlushActive.CAS(false, true) {
		if err := w.flush(); err != nil {
			w.metrics.flushErrors.Inc(1)
		}
		w.isFlushActive.Store(false)
	}

	return w.queue.Close()
}

func (w *writer) QueueSize() int {
	return w.queue.Size()
}

func (w *writer) encodeWithLock(
	encoder *lockedEncoder,
	payload payloadUnion,
) error {
	encoder.Lock()

	var (
		sizeBefore = encoder.Len()
		err        error
	)

	switch payload.payloadType {
	case untimedType:
		err = w.encodeUntimedWithLock(encoder, payload.untimed.metric, payload.untimed.metadatas)
	case forwardedType:
		err = w.encodeForwardedWithLock(encoder, payload.forwarded.metric, payload.forwarded.metadata)
	case timedType:
		err = w.encodeTimedWithLock(encoder, payload.timed.metric, payload.timed.metadata)
	case timedWithStagedMetadatasType:
		elem := payload.timedWithStagedMetadatas
		err = w.encodeTimedWithStagedMetadatasWithLock(encoder, elem.metric, elem.metadatas)
	case passthroughType:
		err = w.encodePassthroughWithLock(encoder, payload.passthrough.metric, payload.passthrough.storagePolicy)
	default:
		err = errUnrecognizedPayloadType
	}

	if err != nil {
		w.metrics.encodeErrors.Inc(1)
		w.log.Error("encode untimed metric error",
			zap.Any("payload", payload),
			zap.Int("payloadType", int(payload.payloadType)),
			zap.Error(err),
		)
		// Rewind buffer and clear out the encoder error.
		encoder.Truncate(sizeBefore) //nolint:errcheck
		encoder.Unlock()
		return err
	}

	encoder.Unlock()

	return nil
}

func (w *writer) encodeUntimedWithLock(
	encoder *lockedEncoder,
	metricUnion unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
	switch metricUnion.Type {
	case metric.CounterType:
		msg := encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         metricUnion.Counter(),
				StagedMetadatas: metadatas,
			}}

		return encoder.EncodeMessage(msg)
	case metric.TimerType:
		// If there is no limit on the timer batch size, write the full batch.
		if w.maxTimerBatchSize == 0 {
			msg := encoding.UnaggregatedMessageUnion{
				Type: encoding.BatchTimerWithMetadatasType,
				BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
					BatchTimer:      metricUnion.BatchTimer(),
					StagedMetadatas: metadatas,
				}}

			return encoder.EncodeMessage(msg)
		}

		// Otherwise, honor maximum timer batch size.
		var (
			batchTimer     = metricUnion.BatchTimer()
			timerValues    = batchTimer.Values
			numTimerValues = len(timerValues)
			start, end     int
		)

		for start = 0; start < numTimerValues; start = end {
			end = start + w.maxTimerBatchSize
			if end > numTimerValues {
				end = numTimerValues
			}
			singleBatchTimer := unaggregated.BatchTimer{
				ID:         batchTimer.ID,
				Values:     timerValues[start:end],
				Annotation: metricUnion.Annotation,
			}
			msg := encoding.UnaggregatedMessageUnion{
				Type: encoding.BatchTimerWithMetadatasType,
				BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
					BatchTimer:      singleBatchTimer,
					StagedMetadatas: metadatas,
				}}
			if err := encoder.EncodeMessage(msg); err != nil {
				return err
			}

			// Unlock the encoder before we encode another metric to ensure other
			// goroutines have an opportunity to encode metrics while larger timer
			// batches are being encoded.
			if end < numTimerValues {
				encoder.Unlock()
				encoder.Lock()
			}
		}

		return nil
	case metric.GaugeType:
		msg := encoding.UnaggregatedMessageUnion{
			Type: encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: unaggregated.GaugeWithMetadatas{
				Gauge:           metricUnion.Gauge(),
				StagedMetadatas: metadatas,
			}}
		return encoder.EncodeMessage(msg)
	default:
	}

	return errUnrecognizedMetricType
}

func (w *writer) encodeForwardedWithLock(
	encoder *lockedEncoder,
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.ForwardedMetricWithMetadataType,
		ForwardedMetricWithMetadata: aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: metric,
			ForwardMetadata: metadata,
		}}

	return encoder.EncodeMessage(msg)
}

func (w *writer) encodeTimedWithLock(
	encoder *lockedEncoder,
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.TimedMetricWithMetadataType,
		TimedMetricWithMetadata: aggregated.TimedMetricWithMetadata{
			Metric:        metric,
			TimedMetadata: metadata,
		}}

	return encoder.EncodeMessage(msg)
}

func (w *writer) encodeTimedWithStagedMetadatasWithLock(
	encoder *lockedEncoder,
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.TimedMetricWithMetadatasType,
		TimedMetricWithMetadatas: aggregated.TimedMetricWithMetadatas{
			Metric:          metric,
			StagedMetadatas: metadatas,
		}}

	return encoder.EncodeMessage(msg)
}

func (w *writer) encodePassthroughWithLock(
	encoder *lockedEncoder,
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.PassthroughMetricWithMetadataType,
		PassthroughMetricWithMetadata: aggregated.PassthroughMetricWithMetadata{
			Metric:        metric,
			StoragePolicy: storagePolicy,
		}}

	return encoder.EncodeMessage(msg)
}

func (w *writer) flush() error {
	multiErr := xerrors.NewMultiError()

	w.encoderByShardMtx.RLock()
	for _, encoder := range w.encodersByShard {
		encoder.Lock()
		if encoder.Len() == 0 {
			encoder.Unlock()
			continue
		}
		buffer := encoder.Relinquish()
		encoder.Unlock()
		if err := w.enqueueBuffer(buffer); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	w.encoderByShardMtx.RUnlock()

	w.queue.Flush()

	return multiErr.FinalError()
}

func (w *writer) enqueueBuffer(buf protobuf.Buffer) error {
	if err := w.queue.Enqueue(buf); err != nil {
		w.metrics.enqueueErrors.Inc(1)
		return err
	}
	w.metrics.buffersEnqueued.Inc(1)
	return nil
}

const (
	buffersMetric = "buffers"
	actionTag     = "action"
)

type writerMetrics struct {
	buffersEnqueued tally.Counter
	encodeErrors    tally.Counter
	enqueueErrors   tally.Counter
	flushErrors     tally.Counter
	skippedFlushes  tally.Counter
}

func newWriterMetrics(s tally.Scope) writerMetrics {
	return writerMetrics{
		buffersEnqueued: s.Tagged(map[string]string{actionTag: "enqueued"}).Counter(buffersMetric),
		encodeErrors:    s.Tagged(map[string]string{actionTag: "encode-error"}).Counter(buffersMetric),
		enqueueErrors:   s.Tagged(map[string]string{actionTag: "enqueue-error"}).Counter(buffersMetric),
		flushErrors:     s.Tagged(map[string]string{actionTag: "flush-error"}).Counter(buffersMetric),
		skippedFlushes:  s.Tagged(map[string]string{actionTag: "skipped-flush"}).Counter(buffersMetric),
	}
}

type lockedEncoder struct {
	protobuf.UnaggregatedEncoder
	sync.Mutex
}

func newLockedEncoder(encoderOpts protobuf.UnaggregatedOptions) *lockedEncoder {
	encoder := protobuf.NewUnaggregatedEncoder(encoderOpts)
	return &lockedEncoder{UnaggregatedEncoder: encoder}
}

type refCountedWriter struct {
	instanceWriter
	refCount
}

func newRefCountedWriter(instance placement.Instance, opts Options) *refCountedWriter {
	rcWriter := &refCountedWriter{instanceWriter: newInstanceWriter(instance, opts)}
	rcWriter.refCount.SetDestructor(rcWriter.Close)
	return rcWriter
}

func (rcWriter *refCountedWriter) Close() {
	// The following Close() used to be called asyncly
	// since Close() grabbed a lock before but it does not
	// anymore so we can simply call it in the same
	// context. This is called when the writeMgr is
	// shutting down.
	rcWriter.instanceWriter.Close() // nolint: errcheck
}
