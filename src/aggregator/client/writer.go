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
	"fmt"
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
	"go.uber.org/zap"
)

var (
	errInstanceWriterClosed   = errors.New("instance writer is closed")
	errUnrecognizedMetricType = errors.New("unrecognized metric type")
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
	sync.RWMutex

	log               *zap.Logger
	metrics           writerMetrics
	encoderOpts       protobuf.UnaggregatedOptions
	queue             instanceQueue
	flushSize         int
	maxTimerBatchSize int

	encodersByShard    map[uint32]*lockedEncoder
	newLockedEncoderFn newLockedEncoderFn
	closed             bool
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
		flushSize:         opts.FlushSize(),
		maxTimerBatchSize: opts.MaxTimerBatchSize(),
		encoderOpts:       opts.EncoderOptions(),
		queue:             newInstanceQueue(instance, queueOpts),
		encodersByShard:   make(map[uint32]*lockedEncoder),
	}
	w.newLockedEncoderFn = newLockedEncoder
	return w
}

func (w *writer) Write(shard uint32, payload payloadUnion) error {
	w.RLock()
	if w.closed {
		w.RUnlock()
		return errInstanceWriterClosed
	}
	encoder, exists := w.encodersByShard[shard]
	if exists {
		err := w.encodeWithLock(encoder, payload)
		w.RUnlock()
		return err
	}
	w.RUnlock()

	w.Lock()
	if w.closed {
		w.Unlock()
		return errInstanceWriterClosed
	}
	encoder, exists = w.encodersByShard[shard]
	if exists {
		err := w.encodeWithLock(encoder, payload)
		w.Unlock()
		return err
	}
	encoder = w.newLockedEncoderFn(w.encoderOpts)
	w.encodersByShard[shard] = encoder
	err := w.encodeWithLock(encoder, payload)
	w.Unlock()

	return err
}

func (w *writer) Flush() error {
	w.RLock()
	if w.closed {
		w.RUnlock()
		return errInstanceWriterClosed
	}
	err := w.flushWithLock()
	w.RUnlock()

	if err != nil {
		w.metrics.flushErrors.Inc(1)
		return err
	}
	return nil
}

func (w *writer) Close() error {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return errInstanceWriterClosed
	}
	w.closed = true
	if err := w.flushWithLock(); err != nil {
		w.metrics.flushErrors.Inc(1)
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
	switch payload.payloadType {
	case untimedType:
		return w.encodeUntimedWithLock(encoder, payload.untimed.metric, payload.untimed.metadatas)
	case forwardedType:
		return w.encodeForwardedWithLock(encoder, payload.forwarded.metric, payload.forwarded.metadata)
	case timedType:
		return w.encodeTimedWithLock(encoder, payload.timed.metric, payload.timed.metadata)
	case timedWithStagedMetadatasType:
		elem := payload.timedWithStagedMetadatas
		return w.encodeTimedWithStagedMetadatasWithLock(encoder, elem.metric, elem.metadatas)
	case passthroughType:
		return w.encodePassthroughWithLock(encoder, payload.passthrough.metric, payload.passthrough.storagePolicy)
	default:
		return fmt.Errorf("unknown payload type: %v", payload.payloadType)
	}
}

func (w *writer) encodeUntimedWithLock(
	encoder *lockedEncoder,
	metricUnion unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
	encoder.Lock()

	var (
		sizeBefore = encoder.Len()
		encodeErr  error
		enqueueErr error
	)
	switch metricUnion.Type {
	case metric.CounterType:
		msg := encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         metricUnion.Counter(),
				StagedMetadatas: metadatas,
			}}
		encodeErr = encoder.EncodeMessage(msg)
	case metric.TimerType:
		// If there is no limit on the timer batch size, write the full batch.
		if w.maxTimerBatchSize == 0 {
			msg := encoding.UnaggregatedMessageUnion{
				Type: encoding.BatchTimerWithMetadatasType,
				BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
					BatchTimer:      metricUnion.BatchTimer(),
					StagedMetadatas: metadatas,
				}}
			encodeErr = encoder.EncodeMessage(msg)
			break
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
				ID:     batchTimer.ID,
				Values: timerValues[start:end],
			}
			msg := encoding.UnaggregatedMessageUnion{
				Type: encoding.BatchTimerWithMetadatasType,
				BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
					BatchTimer:      singleBatchTimer,
					StagedMetadatas: metadatas,
				}}
			encodeErr = encoder.EncodeMessage(msg)
			if encodeErr != nil {
				break
			}

			// If the buffer isn't big enough continue to the next iteration.
			if sizeAfter := encoder.Len(); sizeAfter < w.flushSize {
				continue
			}

			// Otherwise we enqueue the current buffer.
			buffer := w.prepareEnqueueBufferWithLock(encoder, sizeBefore)

			// Unlock the encoder before we enqueue the old buffer to ensure other
			// goroutines have an oppurtunity to encode metrics while larger timer
			// batches are being encoded.
			encoder.Unlock()

			enqueueErr = w.enqueueBuffer(buffer)

			// Re-lock the encoder and update variables since the encoder's buffer
			// may have been updated.
			encoder.Lock()
			sizeBefore = encoder.Len()

			if enqueueErr != nil {
				break
			}
		}
	case metric.GaugeType:
		msg := encoding.UnaggregatedMessageUnion{
			Type: encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: unaggregated.GaugeWithMetadatas{
				Gauge:           metricUnion.Gauge(),
				StagedMetadatas: metadatas,
			}}
		encodeErr = encoder.EncodeMessage(msg)
	default:
		encodeErr = errUnrecognizedMetricType
	}

	if encodeErr != nil {
		w.log.Error("encode untimed metric error",
			zap.Any("metric", metricUnion),
			zap.Any("metadatas", metadatas),
			zap.Error(encodeErr),
		)
		// Rewind buffer and clear out the encoder error.
		encoder.Truncate(sizeBefore)
		encoder.Unlock()
		w.metrics.encodeErrors.Inc(1)
		return encodeErr
	}

	if enqueueErr != nil {
		encoder.Unlock()
		return enqueueErr
	}

	// If the buffer size is not big enough, do nothing.
	if sizeAfter := encoder.Len(); sizeAfter < w.flushSize {
		encoder.Unlock()
		return nil
	}

	// Otherwise we enqueue the current buffer.
	buffer := w.prepareEnqueueBufferWithLock(encoder, sizeBefore)
	encoder.Unlock()
	return w.enqueueBuffer(buffer)
}

func (w *writer) encodeForwardedWithLock(
	encoder *lockedEncoder,
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	encoder.Lock()

	sizeBefore := encoder.Len()
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.ForwardedMetricWithMetadataType,
		ForwardedMetricWithMetadata: aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: metric,
			ForwardMetadata: metadata,
		}}
	if err := encoder.EncodeMessage(msg); err != nil {
		w.log.Error("encode forwarded metric error",
			zap.Any("metric", metric),
			zap.Any("metadata", metadata),
			zap.Error(err),
		)
		// Rewind buffer and clear out the encoder error.
		encoder.Truncate(sizeBefore)
		encoder.Unlock()
		w.metrics.encodeErrors.Inc(1)
		return err
	}

	// If the buffer size is not big enough, do nothing.
	if sizeAfter := encoder.Len(); sizeAfter < w.flushSize {
		encoder.Unlock()
		return nil
	}

	// Otherwise we enqueue the current buffer.
	buffer := w.prepareEnqueueBufferWithLock(encoder, sizeBefore)
	encoder.Unlock()
	return w.enqueueBuffer(buffer)
}

func (w *writer) encodeTimedWithLock(
	encoder *lockedEncoder,
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	encoder.Lock()

	sizeBefore := encoder.Len()
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.TimedMetricWithMetadataType,
		TimedMetricWithMetadata: aggregated.TimedMetricWithMetadata{
			Metric:        metric,
			TimedMetadata: metadata,
		}}
	if err := encoder.EncodeMessage(msg); err != nil {
		w.log.Error("encode timed metric error",
			zap.Any("metric", metric),
			zap.Any("metadata", metadata),
			zap.Error(err),
		)
		// Rewind buffer and clear out the encoder error.
		encoder.Truncate(sizeBefore)
		encoder.Unlock()
		w.metrics.encodeErrors.Inc(1)
		return err
	}

	// If the buffer size is not big enough, do nothing.
	if sizeAfter := encoder.Len(); sizeAfter < w.flushSize {
		encoder.Unlock()
		return nil
	}

	// Otherwise we enqueue the current buffer.
	buffer := w.prepareEnqueueBufferWithLock(encoder, sizeBefore)
	encoder.Unlock()
	return w.enqueueBuffer(buffer)
}

func (w *writer) encodeTimedWithStagedMetadatasWithLock(
	encoder *lockedEncoder,
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	encoder.Lock()

	sizeBefore := encoder.Len()
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.TimedMetricWithMetadatasType,
		TimedMetricWithMetadatas: aggregated.TimedMetricWithMetadatas{
			Metric:          metric,
			StagedMetadatas: metadatas,
		}}
	if err := encoder.EncodeMessage(msg); err != nil {
		w.log.Error("encode timed metric error",
			zap.Any("metric", metric),
			zap.Any("metadatas", metadatas),
			zap.Error(err),
		)
		// Rewind buffer and clear out the encoder error.
		encoder.Truncate(sizeBefore)
		encoder.Unlock()
		w.metrics.encodeErrors.Inc(1)
		return err
	}

	// If the buffer size is not big enough, do nothing.
	if sizeAfter := encoder.Len(); sizeAfter < w.flushSize {
		encoder.Unlock()
		return nil
	}

	// Otherwise we enqueue the current buffer.
	buffer := w.prepareEnqueueBufferWithLock(encoder, sizeBefore)
	encoder.Unlock()
	return w.enqueueBuffer(buffer)
}

func (w *writer) encodePassthroughWithLock(
	encoder *lockedEncoder,
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	encoder.Lock()

	sizeBefore := encoder.Len()
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.PassthroughMetricWithMetadataType,
		PassthroughMetricWithMetadata: aggregated.PassthroughMetricWithMetadata{
			Metric:        metric,
			StoragePolicy: storagePolicy,
		}}
	if err := encoder.EncodeMessage(msg); err != nil {
		w.log.Error("encode passthrough metric error",
			zap.Any("metric", metric),
			zap.Any("storagepolicy", storagePolicy),
			zap.Error(err),
		)
		// Rewind buffer and clear out the encoder error.
		encoder.Truncate(sizeBefore)
		encoder.Unlock()
		w.metrics.encodeErrors.Inc(1)
		return err
	}

	// If the buffer size is not big enough, do nothing.
	if sizeAfter := encoder.Len(); sizeAfter < w.flushSize {
		encoder.Unlock()
		return nil
	}

	// Otherwise we enqueue the current buffer.
	buffer := w.prepareEnqueueBufferWithLock(encoder, sizeBefore)
	encoder.Unlock()
	return w.enqueueBuffer(buffer)
}

// prepareEnqueueBufferWithLock prepares the writer to enqueue a
// buffer onto its instance queue. It gets a new buffer from pool,
// copies the bytes exceeding sizeBefore to it, resets the encoder
// with the new buffer, and returns the old buffer.
func (w *writer) prepareEnqueueBufferWithLock(
	encoder *lockedEncoder,
	sizeBefore int,
) protobuf.Buffer {
	buf := encoder.Relinquish()
	if sizeBefore == 0 {
		// If a single write causes the buffer to exceed the flush size,
		// reset and send the buffer as is.
		return buf
	}
	// Otherwise we reset the buffer and copy the bytes exceeding sizeBefore,
	// and return the old buffer.
	encoder.Reset(buf.Bytes()[sizeBefore:])
	buf.Truncate(sizeBefore)
	return buf
}

func (w *writer) flushWithLock() error {
	multiErr := xerrors.NewMultiError()
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
}

func newWriterMetrics(s tally.Scope) writerMetrics {
	return writerMetrics{
		buffersEnqueued: s.Tagged(map[string]string{actionTag: "enqueued"}).Counter(buffersMetric),
		encodeErrors:    s.Tagged(map[string]string{actionTag: "encode-error"}).Counter(buffersMetric),
		enqueueErrors:   s.Tagged(map[string]string{actionTag: "enqueue-error"}).Counter(buffersMetric),
		flushErrors:     s.Tagged(map[string]string{actionTag: "flush-error"}).Counter(buffersMetric),
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
	// NB: closing the writer needs to be done asynchronously because it may
	// be called by writer manager while holding a lock that blocks any writes
	// from proceeding.
	go rcWriter.instanceWriter.Close() // nolint: errcheck
}
