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
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet"
	"github.com/uber-go/tally"
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

	_ gnet.EventHandler = (*connHandler)(nil)

	pbSlicePool = &sync.Pool{
		New: func() interface{} {
			pb := make([]metricpb.MetricWithMetadatas, _pbSlicePoolCap)
			return &pb
		},
	}
)

type decodeError error

type connHandler struct {
	gnet.EventServer
	agg     aggregator.Aggregator
	logger  *zap.Logger
	p       *ants.Pool
	_       cpu.CacheLinePad
	metrics handlerMetrics
	_       cpu.CacheLinePad
	limiter *rate.Limiter
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

func (h *connHandler) React(frame []byte, c gnet.Conn) ([]byte, gnet.Action) {
	var (
		buf  = frame
		bufs = make([][]byte, 0, _pbSlicePoolCap)
	)

	if len(frame) < binary.MaxVarintLen32 {
		h.handleErr("decode error", errDecodeTooSmall, c)
		return nil, gnet.Close
	}

	for len(buf) >= binary.MaxVarintLen32 {
		size, n := binary.Varint(buf)
		payloadLen := int(size) + n

		if n <= 0 || payloadLen > len(buf) || payloadLen > _maxPayloadSize {
			h.handleErr("decode error", errDecodePayloadSize, c)
			break
		}

		bufs = append(bufs, buf[n:payloadLen])
		buf = buf[payloadLen:]
	}

	if len(buf) > 0 {
		h.handleErr("decode error", errPayloadNotConsumed, c)
		return nil, gnet.Close
	}

	metrics := *pbSlicePool.Get().(*[]metricpb.MetricWithMetadatas) //nolint:errcheck
	if cap(metrics) <= len(bufs) {
		metrics = metrics[0:cap(metrics)]
	} else {
		pbSlicePool.Put(&metrics)
		metrics = make([]metricpb.MetricWithMetadatas, len(bufs))
	}

	for i := range bufs {
		pb := &metrics[i]
		pb.Reset() // no point reusing here, because proto unmarshaller will always alloc deeply-nested slices/structs
		if err := pb.Unmarshal(bufs[i]); err != nil {
			h.metrics.decodeErrors.Inc(1)
			h.handleErr("decode error", err, c)
			continue
		}
	}

	//nolint:errcheck
	h.p.Submit(func() { // we're using a non-blocking pool
		h.process(metrics, c)
		pbSlicePool.Put(&metrics)
	})

	return nil, gnet.None
}

func (h *connHandler) process(metrics []metricpb.MetricWithMetadatas, c gnet.Conn) {
	for i := range metrics {
		var (
			current = &metrics[i]
			metric  encoding.UnaggregatedMessageUnion
			err     error
		)

		switch current.Type {
		case metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS:
			m := &metric.CounterWithMetadatas
			if err = m.FromProto(current.CounterWithMetadatas); err == nil {
				err = h.agg.AddUntimed(m.Counter.ToUnion(), m.StagedMetadatas)
			}
		case metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS:
			m := &metric.BatchTimerWithMetadatas
			if err = m.FromProto(current.BatchTimerWithMetadatas); err == nil {
				err = h.agg.AddUntimed(m.BatchTimer.ToUnion(), m.StagedMetadatas)
			}
		case metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS:
			m := &metric.GaugeWithMetadatas
			if err = m.FromProto(current.GaugeWithMetadatas); err == nil {
				err = h.agg.AddUntimed(m.Gauge.ToUnion(), m.StagedMetadatas)
			}
		case metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA:
			m := &metric.ForwardedMetricWithMetadata
			if err = m.FromProto(current.ForwardedMetricWithMetadata); err == nil {
				err = h.agg.AddForwarded(m.ForwardedMetric, m.ForwardMetadata)
			}
		case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA:
			m := &metric.TimedMetricWithMetadata
			if err = m.FromProto(current.TimedMetricWithMetadata); err == nil {
				err = h.agg.AddTimed(m.Metric, m.TimedMetadata)
			}
		case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS:
			m := &metric.TimedMetricWithMetadatas
			if err = m.FromProto(current.TimedMetricWithMetadatas); err == nil {
				err = h.agg.AddTimedWithStagedMetadatas(m.Metric, m.StagedMetadatas)
			}
		case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_STORAGE_POLICY:
			m := &metric.PassthroughMetricWithMetadata
			if err = m.FromProto(current.TimedMetricWithStoragePolicy); err == nil {
				err = h.agg.AddPassthrough(m.Metric, m.StoragePolicy)
			}
		default:
			err = newUnknownMessageTypeError(encoding.UnaggregatedMessageType(current.Type))
		}

		if err != nil {
			metric.Type = protobuf.TypeFromProto(current.Type)
			h.handleErr("error adding metric", addMetricError{
				error:  err,
				metric: &metric,
			}, c)
		}
	}
}

//nolint:gocyclo
func (h *connHandler) handleErr(msg string, err error, c gnet.Conn) {
	var metric *encoding.UnaggregatedMessageUnion

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
		case encoding.BatchTimerWithMetadatasType:
			h.metrics.addUntimedErrors.Inc(1)
		case encoding.GaugeWithMetadatasType:
			h.metrics.addUntimedErrors.Inc(1)
		case encoding.ForwardedMetricWithMetadataType:
			h.metrics.addForwardedErrors.Inc(1)
		case encoding.TimedMetricWithMetadataType:
			h.metrics.addTimedErrors.Inc(1)
		case encoding.TimedMetricWithMetadatasType:
			h.metrics.addTimedErrors.Inc(1)
		case encoding.PassthroughMetricWithMetadataType:
			h.metrics.addPassthroughErrors.Inc(1)
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

	fields := make([]zap.Field, 0, 10)
	fields = append(
		fields,
		zap.Stringer("remoteAddress", c.RemoteAddr()),
		zap.Error(err))

	if metric == nil {
		h.logger.Info(msg, fields...)
		return
	}

	var (
		mid       id.RawID
		value     interface{}
		timestamp int64
	)

	switch metric.Type {
	case encoding.CounterWithMetadatasType:
		mid = metric.CounterWithMetadatas.ID
		value = metric.CounterWithMetadatas.Value
	case encoding.BatchTimerWithMetadatasType:
		mid = metric.BatchTimerWithMetadatas.ID
		value = metric.BatchTimerWithMetadatas.Values
	case encoding.GaugeWithMetadatasType:
		mid = metric.GaugeWithMetadatas.ID
		value = metric.GaugeWithMetadatas.Value
	case encoding.ForwardedMetricWithMetadataType:
		mid = metric.ForwardedMetricWithMetadata.ID
		value = metric.ForwardedMetricWithMetadata.Values
		timestamp = metric.ForwardedMetricWithMetadata.TimeNanos
	case encoding.TimedMetricWithMetadataType:
		mid = metric.TimedMetricWithMetadata.ID
		value = metric.TimedMetricWithMetadata.Value
		timestamp = metric.TimedMetricWithMetadata.TimeNanos
	case encoding.TimedMetricWithMetadatasType:
		mid = metric.TimedMetricWithMetadatas.ID
		value = metric.TimedMetricWithMetadatas.Value
		timestamp = metric.TimedMetricWithMetadatas.TimeNanos
	case encoding.PassthroughMetricWithMetadataType:
		mid = metric.PassthroughMetricWithMetadata.ID
		value = metric.PassthroughMetricWithMetadata.Value
		timestamp = metric.PassthroughMetricWithMetadata.TimeNanos
	case encoding.UnknownMessageType:
	}

	fields = append(fields,
		zap.Int("metricType", int(metric.Type)),
		zap.Stringer("id", mid),
		zap.Time("timestamp", time.Unix(0, timestamp)),
		zap.Any("values", value),
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
