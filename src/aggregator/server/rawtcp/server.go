// Copyright (c) 2016 Uber Technologies, Inc.
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

package rawtcp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/rate"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	xio "github.com/m3db/m3/src/x/io"
	xserver "github.com/m3db/m3/src/x/server"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	unknownRemoteHostAddress = "<unknown>"
)

// NewServer creates a new raw TCP server.
func NewServer(address string, aggregator aggregator.Aggregator, opts Options) xserver.Server {
	iOpts := opts.InstrumentOptions()
	handlerScope := iOpts.MetricsScope().Tagged(map[string]string{"handler": "rawtcp"})
	handler := NewHandler(aggregator, opts.SetInstrumentOptions(iOpts.SetMetricsScope(handlerScope)))
	return xserver.NewServer(address, handler, opts.ServerOptions())
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

type handler struct {
	sync.Mutex

	aggregator     aggregator.Aggregator
	log            *zap.Logger
	readBufferSize int
	protobufItOpts protobuf.UnaggregatedOptions

	errLogRateLimiter *rate.Limiter
	metrics           handlerMetrics

	opts Options
}

// NewHandler creates a new raw TCP handler.
func NewHandler(aggregator aggregator.Aggregator, opts Options) xserver.Handler {
	iOpts := opts.InstrumentOptions()
	var limiter *rate.Limiter
	if rateLimit := opts.ErrorLogLimitPerSecond(); rateLimit != 0 {
		limiter = rate.NewLimiter(rateLimit)
	}
	return &handler{
		aggregator:        aggregator,
		log:               iOpts.Logger(),
		readBufferSize:    opts.ReadBufferSize(),
		protobufItOpts:    opts.ProtobufUnaggregatedIteratorOptions(),
		errLogRateLimiter: limiter,
		metrics:           newHandlerMetrics(iOpts.MetricsScope()),
		opts:              opts,
	}
}

func (s *handler) Handle(conn net.Conn) {
	remoteAddress := unknownRemoteHostAddress
	if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
		remoteAddress = remoteAddr.String()
	}

	nowFn := s.opts.ClockOptions().NowFn()
	rOpts := xio.ResettableReaderOptions{ReadBufferSize: s.readBufferSize}
	read := s.opts.RWOptions().ResettableReaderFn()(conn, rOpts)
	reader := bufio.NewReaderSize(read, s.readBufferSize)
	it := protobuf.NewUnaggregatedIterator(reader, s.protobufItOpts)
	defer it.Close()

	// Iterate over the incoming metrics stream and queue up metrics.
	var (
		untimedMetric       unaggregated.MetricUnion
		stagedMetadatas     metadata.StagedMetadatas
		forwardedMetric     aggregated.ForwardedMetric
		forwardMetadata     metadata.ForwardMetadata
		timedMetric         aggregated.Metric
		timedMetadata       metadata.TimedMetadata
		passthroughMetric   aggregated.Metric
		passthroughMetadata policy.StoragePolicy
		err                 error
	)
	for it.Next() {
		current := it.Current()
		switch current.Type {
		case encoding.CounterWithMetadatasType:
			untimedMetric = current.CounterWithMetadatas.Counter.ToUnion()
			stagedMetadatas = current.CounterWithMetadatas.StagedMetadatas
			err = toAddUntimedError(s.aggregator.AddUntimed(untimedMetric, stagedMetadatas))
		case encoding.BatchTimerWithMetadatasType:
			untimedMetric = current.BatchTimerWithMetadatas.BatchTimer.ToUnion()
			stagedMetadatas = current.BatchTimerWithMetadatas.StagedMetadatas
			err = toAddUntimedError(s.aggregator.AddUntimed(untimedMetric, stagedMetadatas))
		case encoding.GaugeWithMetadatasType:
			untimedMetric = current.GaugeWithMetadatas.Gauge.ToUnion()
			stagedMetadatas = current.GaugeWithMetadatas.StagedMetadatas
			err = toAddUntimedError(s.aggregator.AddUntimed(untimedMetric, stagedMetadatas))
		case encoding.ForwardedMetricWithMetadataType:
			forwardedMetric = current.ForwardedMetricWithMetadata.ForwardedMetric
			forwardMetadata = current.ForwardedMetricWithMetadata.ForwardMetadata
			err = toAddForwardedError(s.aggregator.AddForwarded(forwardedMetric, forwardMetadata))
		case encoding.TimedMetricWithMetadataType:
			timedMetric = current.TimedMetricWithMetadata.Metric
			timedMetadata = current.TimedMetricWithMetadata.TimedMetadata
			err = toAddTimedError(s.aggregator.AddTimed(timedMetric, timedMetadata))
		case encoding.TimedMetricWithMetadatasType:
			timedMetric = current.TimedMetricWithMetadatas.Metric
			stagedMetadatas = current.TimedMetricWithMetadatas.StagedMetadatas
			err = toAddTimedError(s.aggregator.AddTimedWithStagedMetadatas(timedMetric, stagedMetadatas))
		case encoding.PassthroughMetricWithMetadataType:
			passthroughMetric = current.PassthroughMetricWithMetadata.Metric
			passthroughMetadata = current.PassthroughMetricWithMetadata.StoragePolicy
			err = toAddPassthroughError(s.aggregator.AddPassthrough(passthroughMetric, passthroughMetadata))
		default:
			err = newUnknownMessageTypeError(current.Type)
		}

		if err == nil {
			continue
		}

		// We rate limit the error log here because the error rate may scale with
		// the metrics incoming rate and consume lots of cpu cycles.
		if s.errLogRateLimiter != nil && !s.errLogRateLimiter.IsAllowed(1, xtime.ToUnixNano(nowFn())) {
			s.metrics.errLogRateLimited.Inc(1)
			continue
		}
		switch err.(type) {
		case unknownMessageTypeError:
			s.metrics.unknownMessageTypeErrors.Inc(1)
			s.log.Error("unexpected message type",
				zap.String("remoteAddress", remoteAddress),
				zap.Error(err),
			)
		case addUntimedError:
			s.metrics.addUntimedErrors.Inc(1)
			s.log.Error("error adding untimed metric",
				zap.String("remoteAddress", remoteAddress),
				zap.Stringer("type", untimedMetric.Type),
				zap.Stringer("id", untimedMetric.ID),
				zap.Any("metadatas", stagedMetadatas),
				zap.Error(err),
			)
		case addForwardedError:
			s.metrics.addForwardedErrors.Inc(1)
			s.log.Error("error adding forwarded metric",
				zap.String("remoteAddress", remoteAddress),
				zap.Stringer("id", forwardedMetric.ID),
				zap.Time("timestamp", time.Unix(0, forwardedMetric.TimeNanos)),
				zap.Float64s("values", forwardedMetric.Values),
				zap.Error(err),
			)
		case addTimedError:
			s.metrics.addTimedErrors.Inc(1)
			s.log.Error("error adding timed metric",
				zap.String("remoteAddress", remoteAddress),
				zap.Stringer("id", timedMetric.ID),
				zap.Time("timestamp", time.Unix(0, timedMetric.TimeNanos)),
				zap.Float64("value", timedMetric.Value),
				zap.Error(err),
			)
		case addPassthroughError:
			s.metrics.addPassthroughErrors.Inc(1)
			s.log.Error("error adding passthrough metric",
				zap.String("remoteAddress", remoteAddress),
				zap.Stringer("id", timedMetric.ID),
				zap.Time("timestamp", time.Unix(0, timedMetric.TimeNanos)),
				zap.Float64("value", timedMetric.Value),
				zap.Error(err),
			)
		default:
			s.metrics.unknownErrorTypeErrors.Inc(1)
			s.log.Error("unknown error type",
				zap.String("errorType", fmt.Sprintf("%T", err)),
				zap.Error(err),
			)
		}
	}

	// If there is an error during decoding, it's likely due to a broken connection
	// and therefore we ignore the EOF error.
	if err := it.Err(); err != nil && err != io.EOF {
		s.log.Error("decode error",
			zap.String("remoteAddress", remoteAddress),
			zap.Error(err),
		)
		s.metrics.decodeErrors.Inc(1)
	}
}

func (s *handler) Close() {
	// NB(cw) Do not close s.aggregator here because it's shared between
	// the raw TCP server and the http server, and it will be closed on
	// exit signal.
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

type addUntimedError struct {
	err error
}

func toAddUntimedError(err error) error {
	if err == nil {
		return nil
	}
	return addUntimedError{err: err}
}

func (e addUntimedError) Error() string { return e.err.Error() }

type addTimedError struct {
	err error
}

func toAddTimedError(err error) error {
	if err == nil {
		return nil
	}
	return addTimedError{err: err}
}

func (e addTimedError) Error() string { return e.err.Error() }

type addForwardedError struct {
	err error
}

func toAddForwardedError(err error) error {
	if err == nil {
		return nil
	}
	return addForwardedError{err: err}
}

func (e addForwardedError) Error() string { return e.err.Error() }

type addPassthroughError struct {
	err error
}

func toAddPassthroughError(err error) error {
	if err == nil {
		return nil
	}
	return addPassthroughError{err: err}
}

func (e addPassthroughError) Error() string { return e.err.Error() }
