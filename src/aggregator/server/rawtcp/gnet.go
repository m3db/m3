// Copyright (c) 2021 Uber Technologies, Inc.
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

// Package rawtcp is raw protobuf over tcp server
package rawtcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Allenxuxu/gev"
	"github.com/Allenxuxu/gev/connection"
	"github.com/Allenxuxu/ringbuffer"
	"github.com/panjf2000/ants/v2"
	"github.com/uber-go/tally"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sys/cpu"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/rate"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metric/id"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	_pbSlicePoolCap = 32
	_maxPayloadSize = 2 << 24
)

var (
	errDecodeTooSmall     decodeError = errors.New("error decoding payload: payload too short")
	errDecodePayloadSize  decodeError = errors.New("error decoding payload size")
	errPayloadNotConsumed decodeError = errors.New("payload not fully consumed")

	_ gev.Handler         = (*connHandler)(nil)
	_ connection.Protocol = (*connHandler)(nil)

	pbSlicePool = &sync.Pool{
		New: func() interface{} {
			pb := make([]metricpb.MetricWithMetadatas, _pbSlicePoolCap)
			return &pb
		},
	}

	byteSlicePool = &sync.Pool{
		New: func() interface{} {
			b := make([]byte, 65536)
			return &b
		},
	}

	payloadPool = &sync.Pool{
		New: func() interface{} {
			return &payload{}
		},
	}
)

type payload struct {
	message []byte
	conn    *connection.Connection
}

type decodeError error

type connHandler struct {
	doneCh  chan struct{}
	logger  *zap.Logger
	agg     aggregator.Aggregator
	p       *ants.Pool
	_       cpu.CacheLinePad
	metrics handlerMetrics
	_       cpu.CacheLinePad
	limiter *rate.Limiter
	_       cpu.CacheLinePad
	bufCh   chan *payload
}

// NewConnHandler returns new connection handler
func NewConnHandler(
	aggregator aggregator.Aggregator,
	pool *ants.Pool,
	logger *zap.Logger,
	scope tally.Scope,
	errLogLimit int64,
) *connHandler {
	return &connHandler{
		agg:     aggregator,
		logger:  logger,
		p:       pool,
		metrics: newHandlerMetrics(scope),
		limiter: rate.NewLimiter(errLogLimit),
		bufCh:   make(chan *payload, runtime.GOMAXPROCS(0)),
		doneCh:  make(chan struct{}),
	}
}

func (h *connHandler) OnClose(_ *connection.Connection) {}

func (h *connHandler) OnConnect(_ *connection.Connection) {}

func (h *connHandler) OnMessage(c *connection.Connection, _ interface{}, frame []byte) []byte {
	return nil

	if len(frame) == 0 {
		return nil

	}
	w := payloadPool.Get().(*payload)
	if len(w.message) < len(frame) {
		w.message = make([]byte, len(frame))
		w.message = w.message[:copy(w.message, frame)]
	} else {
		w.message = frame
	}
	w.conn = c

	if h.p.Running() == 0 {
		h.p.Submit(h.process) //nolint:errcheck
	}

	select {
	case h.bufCh <- w:
	default:
		// spawn an additional goroutine to process queue
		//nolint:errcheck
		h.p.Submit(func() { // we're using a blocking pool, call can not fail
			h.process()
		})
		//nolint:errcheck
		h.p.Submit(func() {
			h.bufCh <- w
		})
	}

	return nil
}

var v atomic.Int64

func (h *connHandler) UnPacket(c *connection.Connection, buffer *ringbuffer.RingBuffer) (interface{}, []byte) {
	var (
		err      error
		n        int
		consumed int
	)

	if buffer.VirtualLength() < binary.MaxVarintLen32 {
		return nil, nil
	}
	//consumedTotal := 0
	tmpbuf := make([]byte, 16384)
	buf := (*byteSlicePool.Get().(*[]byte))[:0]
	//a, b := buffer.PeekAll()
For:
	for !buffer.IsEmpty() {
		n, _ = buffer.VirtualRead(tmpbuf)
		if n <= 1 { // at least 1 byte for len
			break
		}
		consumed := 0
		for {
			payloadLen, headerLen := binary.Varint(tmpbuf)
			if headerLen <= 0 {
				continue For
			}
			if payloadLen+int64(headerLen+consumed) > int64(n) {
				continue For
			}
		}
	}
	tbuf := tmpbuf
	fmt.Println("total consumed", v.Load())
	fmt.Println("end read.", "buflen", len(buf), "orig buf len", len(tbuf), "consumed", consumed, "delta", len(tbuf)-consumed)

	return nil, nil
}

func (h *connHandler) Packet(_ *connection.Connection, buf []byte) []byte {
	return buf
}

func (h *connHandler) Close() {
	close(h.doneCh)
}

func (h *connHandler) process() {
	var (
		timer = time.NewTimer(_poolRecycleInterval)
	)
	defer timer.Stop()

	for {
		var w *payload
		select {
		case <-timer.C:
			return
		case <-h.doneCh:
			return
		case w = <-h.bufCh:
		}
		buf := w.message
		for len(buf) > 0 {
			var (
				size, n    = binary.Varint(buf)
				payloadLen = int(size) + n
				pb         = &metricpb.MetricWithMetadatas{}
				metric     encoding.UnaggregatedMessageUnion
				err        error
			)
			if size < 0 || n <= 0 {
				fmt.Println(len(buf), "n", n, "pl", payloadLen,
					fmt.Sprintf("%v:%v", n, payloadLen))
				panic("zz")
				h.handleErr("decode error", errDecodeTooSmall, w.conn)
				break
			}
			//fmt.Println(len(w.message), len(buf))
			msg := buf[n:payloadLen]
			buf = buf[payloadLen:]
			//fmt.Println(len(w.message), len(buf), len(msg))

			if err := pb.Unmarshal(msg); err != nil {
				h.handleErr("decode error", err, w.conn)
				continue
			}
			switch pb.Type {
			case metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS:
				if err = metric.CounterWithMetadatas.FromProto(pb.CounterWithMetadatas); err == nil {
					err = h.agg.AddUntimed(metric.CounterWithMetadatas.Counter.ToUnion(), metric.CounterWithMetadatas.StagedMetadatas)
				}
			case metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS:
				if err = metric.BatchTimerWithMetadatas.FromProto(pb.BatchTimerWithMetadatas); err == nil {
					err = h.agg.AddUntimed(metric.BatchTimerWithMetadatas.BatchTimer.ToUnion(), metric.BatchTimerWithMetadatas.StagedMetadatas)
				}
			case metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS:
				if err = metric.GaugeWithMetadatas.FromProto(pb.GaugeWithMetadatas); err == nil {
					err = h.agg.AddUntimed(metric.GaugeWithMetadatas.Gauge.ToUnion(), metric.GaugeWithMetadatas.StagedMetadatas)
				}
			case metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA:
				if err = metric.ForwardedMetricWithMetadata.FromProto(pb.ForwardedMetricWithMetadata); err == nil {
					err = h.agg.AddForwarded(metric.ForwardedMetricWithMetadata.ForwardedMetric, metric.ForwardedMetricWithMetadata.ForwardMetadata)
				}
			case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA:
				if err = metric.TimedMetricWithMetadata.FromProto(pb.TimedMetricWithMetadata); err == nil {
					err = h.agg.AddTimed(metric.TimedMetricWithMetadata.Metric, metric.TimedMetricWithMetadata.TimedMetadata)
				}
			case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS:
				if err = metric.TimedMetricWithMetadatas.FromProto(pb.TimedMetricWithMetadatas); err == nil {
					err = h.agg.AddTimedWithStagedMetadatas(metric.TimedMetricWithMetadatas.Metric, metric.TimedMetricWithMetadatas.StagedMetadatas)
				}
			case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_STORAGE_POLICY:
				if err = metric.PassthroughMetricWithMetadata.FromProto(pb.TimedMetricWithStoragePolicy); err == nil {
					err = h.agg.AddPassthrough(metric.PassthroughMetricWithMetadata.Metric, metric.PassthroughMetricWithMetadata.StoragePolicy)
				}
			default:
				err = newUnknownMessageTypeError(encoding.UnaggregatedMessageType(pb.Type))
			}

			if err != nil {
				metric.Type = protobuf.TypeFromProto(pb.Type)
				h.handleErr("error adding metric", addMetricError{
					error:  err,
					metric: &metric,
				}, w.conn)
			}
		}
		byteSlicePool.Put(&w.message)
		w.conn = nil
		w.message = nil
		payloadPool.Put(w)
	}
}

//nolint:gocyclo
func (h *connHandler) handleErr(msg string, err error, c *connection.Connection) {
	var (
		mid       id.RawID
		timestamp int64
		metric    *encoding.UnaggregatedMessageUnion
	)
	//nolint:errorlint
	switch merr := err.(type) {
	// not using wrapped errors here, just custom types
	case addMetricError:
		metric = merr.metric
		if metric == nil {
			break
		}

		switch metric.Type {
		case encoding.CounterWithMetadatasType:
			h.metrics.addUntimedErrors.Inc(1)
			mid = metric.CounterWithMetadatas.ID
		case encoding.BatchTimerWithMetadatasType:
			h.metrics.addUntimedErrors.Inc(1)
			mid = metric.BatchTimerWithMetadatas.ID
		case encoding.GaugeWithMetadatasType:
			h.metrics.addUntimedErrors.Inc(1)
			mid = metric.GaugeWithMetadatas.ID
		case encoding.ForwardedMetricWithMetadataType:
			h.metrics.addForwardedErrors.Inc(1)
			mid = metric.ForwardedMetricWithMetadata.ID
			timestamp = metric.ForwardedMetricWithMetadata.TimeNanos
		case encoding.TimedMetricWithMetadataType:
			h.metrics.addTimedErrors.Inc(1)
			mid = metric.TimedMetricWithMetadata.ID
			timestamp = metric.TimedMetricWithMetadata.TimeNanos
		case encoding.TimedMetricWithMetadatasType:
			h.metrics.addTimedErrors.Inc(1)
			mid = metric.TimedMetricWithMetadatas.ID
			timestamp = metric.TimedMetricWithMetadatas.TimeNanos
		case encoding.PassthroughMetricWithMetadataType:
			h.metrics.addPassthroughErrors.Inc(1)
			mid = metric.PassthroughMetricWithMetadata.ID
			timestamp = metric.PassthroughMetricWithMetadata.TimeNanos
		case encoding.UnknownMessageType:
			h.metrics.unknownErrorTypeErrors.Inc(1)
		}
	default:
		h.metrics.decodeErrors.Inc(1)
	}

	if !h.limiter.IsAllowed(1, xtime.UnixNano(time.Now().UnixNano())) {
		h.metrics.errLogRateLimited.Inc(1)
		return
	}

	addr := "<unknown>"
	if c != nil {
		addr = c.PeerAddr()
	}

	fields := make([]zap.Field, 0, 10)
	fields = append(
		fields,
		zap.String("remoteAddress", addr),
		zap.Error(err))

	if metric == nil {
		h.logger.Info(msg, fields...)
		return
	}

	fields = append(fields,
		zap.Int("metricType", int(metric.Type)),
		zap.Stringer("id", mid),
		zap.Time("timestamp", time.Unix(0, timestamp)),
	)

	h.logger.Info(msg, fields...)
}

type handlerMetrics struct {
	unknownMessageTypeErrors tally.Counter
	addUntimedErrors         tally.Counter
	addTimedErrors           tally.Counter
	addForwardedErrors       tally.Counter
	addPassthroughErrors     tally.Counter
	unknownErrorTypeErrors   tally.Counter
	decodeErrors             tally.Counter
	errLogRateLimited        tally.Counter
}

func newHandlerMetrics(scope tally.Scope) handlerMetrics {
	return handlerMetrics{
		unknownMessageTypeErrors: scope.Counter("unknown-message-type-errors"),
		addUntimedErrors:         scope.Counter("add-untimed-errors"),
		addTimedErrors:           scope.Counter("add-timed-errors"),
		addForwardedErrors:       scope.Counter("add-forwarded-errors"),
		addPassthroughErrors:     scope.Counter("add-passthrough-errors"),
		unknownErrorTypeErrors:   scope.Counter("unknown-error-type-errors"),
		decodeErrors:             scope.Counter("decode-errors"),
		errLogRateLimited:        scope.Counter("error-log-rate-limited"),
	}
}

type unknownMessageTypeError struct {
	msgType encoding.UnaggregatedMessageType
}

func newUnknownMessageTypeError(
	msgType encoding.UnaggregatedMessageType,
) unknownMessageTypeError {
	return unknownMessageTypeError{msgType: msgType}
}

func (e unknownMessageTypeError) Error() string {
	return fmt.Sprintf("unknown message type %v", e.msgType)
}

type addMetricError struct {
	error
	metric *encoding.UnaggregatedMessageUnion
}

type poolZapLogger struct {
	logger *zap.Logger
}

func (p poolZapLogger) Printf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	p.logger.Warn("got error from ants worker pool", zap.String("error", msg))
}
