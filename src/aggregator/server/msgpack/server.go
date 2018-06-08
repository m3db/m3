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

package msgpack

import (
	"io"
	"math/rand"
	"net"
	"sync"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/rate"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"
	xserver "github.com/m3db/m3x/server"

	"github.com/uber-go/tally"
)

const (
	unknownRemoteHostAddress = "<unknown>"
)

// NewServer creates a new msgpack server.
func NewServer(address string, aggregator aggregator.Aggregator, opts Options) xserver.Server {
	iOpts := opts.InstrumentOptions()
	handlerScope := iOpts.MetricsScope().Tagged(map[string]string{"handler": "msgpack"})
	handler := NewHandler(aggregator, opts.SetInstrumentOptions(iOpts.SetMetricsScope(handlerScope)))
	return xserver.NewServer(address, handler, opts.ServerOptions())
}

type handlerMetrics struct {
	addUntimedErrors  tally.Counter
	decodeErrors      tally.Counter
	errLogRateLimited tally.Counter
}

func newHandlerMetrics(scope tally.Scope) handlerMetrics {
	return handlerMetrics{
		addUntimedErrors:  scope.Counter("add-untimed-errors"),
		decodeErrors:      scope.Counter("decode-errors"),
		errLogRateLimited: scope.Counter("error-log-rate-limited"),
	}
}

type handler struct {
	sync.Mutex

	opts              Options
	log               log.Logger
	errLogRateLimiter *rate.Limiter
	iteratorPool      msgpack.UnaggregatedIteratorPool

	aggregator aggregator.Aggregator
	rand       *rand.Rand
	metrics    handlerMetrics
}

// NewHandler creates a new msgpack handler.
func NewHandler(aggregator aggregator.Aggregator, opts Options) xserver.Handler {
	nowFn := opts.ClockOptions().NowFn()
	iOpts := opts.InstrumentOptions()
	var limiter *rate.Limiter
	if rateLimit := opts.ErrorLogLimitPerSecond(); rateLimit != 0 {
		limiter = rate.NewLimiter(rateLimit, nowFn)
	}
	return &handler{
		opts:              opts,
		log:               iOpts.Logger(),
		errLogRateLimiter: limiter,
		iteratorPool:      opts.IteratorPool(),
		aggregator:        aggregator,
		rand:              rand.New(rand.NewSource(nowFn().UnixNano())),
		metrics:           newHandlerMetrics(iOpts.MetricsScope()),
	}
}

func (s *handler) Handle(conn net.Conn) {
	remoteAddress := unknownRemoteHostAddress
	if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
		remoteAddress = remoteAddr.String()
	}

	it := s.iteratorPool.Get()
	it.Reset(conn)
	defer it.Close()

	// Iterate over the incoming metrics stream and queue up metrics.
	for it.Next() {
		metric := it.Metric()
		// TODO(xichen): change the stagedMetadatas placeholder to use real
		// staged metadatas decoded from the iterator.
		// policiesList := it.PoliciesList()
		stagedMetadatas := metadata.StagedMetadatas{}
		if err := s.aggregator.AddUntimed(metric, stagedMetadatas); err != nil {
			s.metrics.addUntimedErrors.Inc(1)
			// We rate limit the error log here because the error rate may scale with
			// the metrics incoming rate and consume lots of cpu cycles.
			if s.errLogRateLimiter != nil && !s.errLogRateLimiter.IsAllowed(1) {
				s.metrics.errLogRateLimited.Inc(1)
				continue
			}
			s.log.WithFields(
				log.NewField("remoteAddress", remoteAddress),
				log.NewField("type", metric.Type.String()),
				log.NewField("id", metric.ID.String()),
				log.NewField("metadatas", stagedMetadatas),
				log.NewErrField(err),
			).Errorf("error adding untimed metric")
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
	// the msgpack server and the http server, and it will be closed on
	// exit signal.
}
