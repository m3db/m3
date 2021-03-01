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

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"golang.org/x/sys/cpu"

	"github.com/m3db/m3/src/x/unsafe"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/rate"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	_pbSlicePoolCap = 32
	_maxPayloadSize = 512 * 1024 // 512 kB
)

var (
	errDecodeTooSmall     decodeError = errors.New("error decoding payload: unexpected end of payload")
	errDecodePayloadSize  decodeError = errors.New("error decoding payload size")
	errPayloadNotConsumed decodeError = errors.New("payload not fully consumed")

	_ gnet.EventHandler = (*connHandler)(nil)
	_ gnet.ICodec       = (*connHandler)(nil)

	byteSlicePool = &sync.Pool{
		New: func() interface{} {
			b := make([]byte, 65536)
			return &b
		},
	}

	payloadPool = &sync.Pool{
		New: func() interface{} {
			return &payload{
				message: make([]byte, 65536),
			}
		},
	}
)

type payload struct {
	message []byte
	conn    gnet.Conn
}

type bufCh struct {
	ch chan *payload
	_  cpu.CacheLinePad
}

type decodeError error

type connHandler struct {
	gnet.EventServer
	doneCh  chan struct{}
	logger  *zap.Logger
	agg     aggregator.Aggregator
	p       *ants.Pool
	_       cpu.CacheLinePad
	metrics handlerMetrics
	_       cpu.CacheLinePad
	limiter *rate.Limiter
	_       cpu.CacheLinePad
	bufCh   []bufCh
}

// NewConnHandler returns new connection handler
func NewConnHandler(
	aggregator aggregator.Aggregator,
	pool *ants.Pool,
	logger *zap.Logger,
	scope tally.Scope,
	errLogLimit int64,
) *connHandler {
	// we're logging messages in handleErr method only
	logger = logger.WithOptions(zap.AddCallerSkip(1))
	h := &connHandler{
		agg:     aggregator,
		logger:  logger,
		p:       pool,
		metrics: newHandlerMetrics(scope),
		limiter: rate.NewLimiter(errLogLimit),
		doneCh:  make(chan struct{}),
		bufCh:   make([]bufCh, runtime.GOMAXPROCS(0)),
	}
	for i := 0; i < len(h.bufCh); i++ {
		h.bufCh[i].ch = make(chan *payload, 1)
		go h.process(h.bufCh[i].ch)
	}
	return h
}

func (h *connHandler) React(frame []byte, c gnet.Conn) ([]byte, gnet.Action) {
	if len(frame) == 0 {
		return nil, gnet.None
	}

	w := payloadPool.Get().(*payload)
	if len(w.message) < len(frame) {
		// frame size is bounded by decoder checks
		w.message = make([]byte, len(frame))
	}
	w.message = w.message[:copy(w.message, frame)]
	w.conn = c

	i := int(unsafe.Fastrandn(uint32(len(h.bufCh))))
	h.bufCh[i].ch <- w
	return nil, gnet.None
}

func (h *connHandler) Encode(_ gnet.Conn, _ []byte) ([]byte, error) {
	return nil, nil
}

func (h *connHandler) Decode(c gnet.Conn) ([]byte, error) {
	if c.BufferLength() < 1 {
		return nil, nil
	}

	var (
		size     int
		consumed int
		buf      = c.Read()
		tmp      = buf
	)

	for len(tmp) > 0 {
		payloadLen, headerLen := binary.Varint(tmp)
		size = int(payloadLen) + headerLen
		if size > _maxPayloadSize {
			c.ResetBuffer()
			h.handleErr("decode error", errDecodePayloadSize, c)
			return nil, c.Close()
		}
		if size > len(tmp) || size <= 0 || headerLen <= 0 {
			break
		}
		tmp = tmp[size:]
		consumed += size
	}

	if consumed == 0 {
		// must return a nil slice to end the read
		return nil, nil
	}

	c.ShiftN(consumed)
	return buf[:consumed], nil
}

func (h *connHandler) OnInitComplete(srv gnet.Server) gnet.Action {
	h.logger.Info(
		"gnet server init complete",
		zap.String("server_addr", srv.Addr.String()),
		zap.Duration("keep_alive_duration", srv.TCPKeepAlive),
		zap.Int("num_event_loops", srv.NumEventLoop),
	)

	return gnet.None
}

func (h *connHandler) Close() {
	close(h.doneCh)
}

func (h *connHandler) process(bufCh <-chan *payload) {
	var (
		agg       = h.agg
		w         *payload
		metadatas metadata.StagedMetadatas
		pb        metricpb.MetricWithMetadatas
	)
	for {
		select {
		case <-h.doneCh:
			return
		case w = <-bufCh:
		}

		buf := w.message
		for len(buf) > 0 {
			var (
				size, n    = binary.Varint(buf)
				payloadLen = int(size) + n
				err        error
			)
			protobuf.ReuseMetricWithMetadatasProto(&pb)

			if size <= 0 || n <= 0 || payloadLen > len(buf) {
				h.handleErr("decode error", errDecodeTooSmall, w.conn)
				w.conn.Close()
				break
			}

			msg := buf[n:payloadLen]
			buf = buf[payloadLen:]

			if err := pb.Unmarshal(msg); err != nil {
				h.handleErr("decode error", err, w.conn)
				continue
			}

			switch pb.Type {
			case metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS:
				m := unaggregated.Counter{}
				m.FromProto(pb.CounterWithMetadatas.Counter)
				metadatas.FromProto(pb.CounterWithMetadatas.Metadatas)
				err = agg.AddUntimed(m.ToUnion(), metadatas)
			case metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS:
				m := unaggregated.BatchTimer{}
				m.FromProto(pb.BatchTimerWithMetadatas.BatchTimer)
				metadatas.FromProto(pb.BatchTimerWithMetadatas.Metadatas)
				err = agg.AddUntimed(m.ToUnion(), metadatas)
			case metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS:
				m := unaggregated.Gauge{}
				m.FromProto(pb.GaugeWithMetadatas.Gauge)
				metadatas.FromProto(pb.GaugeWithMetadatas.Metadatas)
				err = agg.AddUntimed(m.ToUnion(), metadatas)
			case metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA:
				m := aggregated.ForwardedMetricWithMetadata{}
				m.FromProto(pb.ForwardedMetricWithMetadata)
				err = agg.AddForwarded(m.ForwardedMetric, m.ForwardMetadata)
			case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA:
				m := aggregated.TimedMetricWithMetadata{}
				m.FromProto(pb.TimedMetricWithMetadata)
				err = agg.AddTimed(m.Metric, m.TimedMetadata)
			case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS:
				m := aggregated.TimedMetricWithMetadatas{}
				m.FromProto(pb.TimedMetricWithMetadatas)
				err = agg.AddTimedWithStagedMetadatas(m.Metric, m.StagedMetadatas)
			case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_STORAGE_POLICY:
				m := aggregated.MetricWithStoragePolicy{}
				m.FromProto(*pb.TimedMetricWithStoragePolicy)
				err = agg.AddPassthrough(m.Metric, m.StoragePolicy)
			default:
				err = newUnknownMessageTypeError(encoding.UnaggregatedMessageType(pb.Type))
			}

			if err != nil {
				h.handleErr("error adding metric", addMetricError{
					error:  err,
					metric: pb,
				}, w.conn)
			}
		}
		w.conn = nil
		w.message = w.message[:cap(w.message)]
		payloadPool.Put(w)
	}
}

//nolint:gocyclo
func (h *connHandler) handleErr(msg string, err error, c gnet.Conn) {
	var (
		mid       id.RawID
		timestamp int64
		mtype     metricpb.MetricWithMetadatas_Type
	)
	//nolint:errorlint
	switch merr := err.(type) {
	// not using wrapped errors here, just custom types
	case addMetricError:
		metric := merr.metric
		mtype = metric.Type

		switch metric.Type {
		case metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS:
			h.metrics.addUntimedErrors.Inc(1)
			mid = metric.CounterWithMetadatas.GetCounter().Id
		case metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS:
			h.metrics.addUntimedErrors.Inc(1)
			mid = metric.BatchTimerWithMetadatas.GetBatchTimer().Id
		case metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS:
			h.metrics.addUntimedErrors.Inc(1)
			mid = metric.GaugeWithMetadatas.GetGauge().Id
		case metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA:
			h.metrics.addForwardedErrors.Inc(1)
			mid = metric.ForwardedMetricWithMetadata.GetMetric().Id
			timestamp = metric.ForwardedMetricWithMetadata.GetMetric().TimeNanos
		case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA:
			h.metrics.addTimedErrors.Inc(1)
			mid = metric.TimedMetricWithMetadata.GetMetric().Id
			timestamp = metric.TimedMetricWithMetadata.GetMetric().TimeNanos
		case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS:
			h.metrics.addTimedErrors.Inc(1)
			mid = metric.TimedMetricWithMetadatas.GetMetric().Id
			timestamp = metric.TimedMetricWithMetadatas.GetMetric().TimeNanos
		case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_STORAGE_POLICY:
			h.metrics.addPassthroughErrors.Inc(1)
			mid = metric.TimedMetricWithStoragePolicy.GetTimedMetric().Id
			timestamp = metric.TimedMetricWithStoragePolicy.GetTimedMetric().TimeNanos
		default:
			h.metrics.unknownErrorTypeErrors.Inc(1)
		}
	default:
		h.metrics.decodeErrors.Inc(1)
	}

	if !h.limiter.IsAllowed(1, xtime.UnixNano(time.Now().UnixNano())) {
		h.metrics.errLogRateLimited.Inc(1)
		return
	}

	fields := make([]zap.Field, 0, 10)
	fields = append(fields, zap.Error(err))
	if c != nil {
		fields = append(fields, zap.Stringer("remoteAddress", c.RemoteAddr()))
	}

	if len(mid) == 0 {
		h.logger.Info(msg, fields...)
		return
	}

	fields = append(fields,
		zap.Int("metricType", int(mtype)),
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
	metric metricpb.MetricWithMetadatas
}

type poolZapLogger struct {
	logger *zap.Logger
}

func (p poolZapLogger) Printf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	p.logger.Warn("got error from  worker pool", zap.String("error", msg))
}
