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
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/rate"
	"github.com/m3db/m3metrics/encoding"
	"github.com/m3db/m3metrics/encoding/migration"
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3metrics/encoding/protobuf"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3x/log"
	xserver "github.com/m3db/m3x/server"

	"github.com/uber-go/tally"
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
		unknownErrorTypeErrors:   scope.Counter("unknown-error-type-errors"),
		decodeErrors:             scope.Counter("decode-errors"),
		errLogRateLimited:        scope.Counter("error-log-rate-limited"),
	}
}

type handler struct {
	sync.Mutex

	aggregator     aggregator.Aggregator
	log            log.Logger
	readBufferSize int
	msgpackItOpts  msgpack.UnaggregatedIteratorOptions
	protobufItOpts protobuf.UnaggregatedOptions

	errLogRateLimiter *rate.Limiter
	rand              *rand.Rand
	metrics           handlerMetrics
}

// NewHandler creates a new raw TCP handler.
func NewHandler(aggregator aggregator.Aggregator, opts Options) xserver.Handler {
	nowFn := opts.ClockOptions().NowFn()
	iOpts := opts.InstrumentOptions()
	var limiter *rate.Limiter
	if rateLimit := opts.ErrorLogLimitPerSecond(); rateLimit != 0 {
		limiter = rate.NewLimiter(rateLimit, nowFn)
	}
	return &handler{
		aggregator:        aggregator,
		log:               iOpts.Logger(),
		readBufferSize:    opts.ReadBufferSize(),
		msgpackItOpts:     opts.MsgpackUnaggregatedIteratorOptions(),
		protobufItOpts:    opts.ProtobufUnaggregatedIteratorOptions(),
		errLogRateLimiter: limiter,
		rand:              rand.New(rand.NewSource(nowFn().UnixNano())),
		metrics:           newHandlerMetrics(iOpts.MetricsScope()),
	}
}

func (s *handler) Handle(conn net.Conn) {
	remoteAddress := unknownRemoteHostAddress
	if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
		remoteAddress = remoteAddr.String()
	}

	reader := bufio.NewReaderSize(conn, s.readBufferSize)
	it := migration.NewUnaggregatedIterator(reader, s.msgpackItOpts, s.protobufItOpts)
	defer it.Close()

	// Iterate over the incoming metrics stream and queue up metrics.
	var (
		untimedMetric   unaggregated.MetricUnion
		stagedMetadatas metadata.StagedMetadatas
		forwardedMetric aggregated.ForwardedMetric
		forwardMetadata metadata.ForwardMetadata
		timedMetric     aggregated.Metric
		timedMetadata   metadata.TimedMetadata
		err             error
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
		default:
			err = newUnknownMessageTypeError(current.Type)
		}

		if err == nil {
			continue
		}

		// We rate limit the error log here because the error rate may scale with
		// the metrics incoming rate and consume lots of cpu cycles.
		if s.errLogRateLimiter != nil && !s.errLogRateLimiter.IsAllowed(1) {
			s.metrics.errLogRateLimited.Inc(1)
			continue
		}
		switch err.(type) {
		case unknownMessageTypeError:
			s.metrics.unknownMessageTypeErrors.Inc(1)
			s.log.WithFields(
				log.NewField("remoteAddress", remoteAddress),
				log.NewErrField(err),
			).Error("unexpected message type")
		case addUntimedError:
			s.metrics.addUntimedErrors.Inc(1)
			s.log.WithFields(
				log.NewField("remoteAddress", remoteAddress),
				log.NewField("type", untimedMetric.Type.String()),
				log.NewField("id", untimedMetric.ID.String()),
				log.NewField("metadatas", stagedMetadatas),
				log.NewErrField(err),
			).Error("error adding untimed metric")
		case addForwardedError:
			s.metrics.addForwardedErrors.Inc(1)
			s.log.WithFields(
				log.NewField("remoteAddress", remoteAddress),
				log.NewField("id", forwardedMetric.ID.String()),
				log.NewField("timestamp", time.Unix(0, forwardedMetric.TimeNanos).String()),
				log.NewField("values", forwardedMetric.Values),
				log.NewErrField(err),
			).Error("error adding forwarded metric")
		case addTimedError:
			s.metrics.addTimedErrors.Inc(1)
			s.log.WithFields(
				log.NewField("remoteAddress", remoteAddress),
				log.NewField("id", timedMetric.ID.String()),
				log.NewField("timestamp", time.Unix(0, timedMetric.TimeNanos).String()),
				log.NewField("value", timedMetric.Value),
				log.NewErrField(err),
			).Error("error adding timed metric")
		default:
			s.metrics.unknownErrorTypeErrors.Inc(1)
			s.log.WithFields(
				log.NewField("errorType", fmt.Sprintf("%T", err)),
				log.NewErrField(err),
			).Errorf("unknown error type")
		}
	}

	// If there is an error during decoding, it's likely due to a broken connection
	// and therefore we ignore the EOF error.
	if err := it.Err(); err != nil && err != io.EOF {
		s.log.WithFields(
			log.NewField("remoteAddress", remoteAddress),
			log.NewErrField(err),
		).Error("decode error")
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
