// Copyright (c) 2019 Uber Technologies, Inc.
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

package ingestcarbon

import (
	"net"
	"sync"
	"time"

	"code.uber.internal/infra/statsdex/protocols"
	"code.uber.internal/infra/statsdex/services/m3dbingester/ingest"
	"github.com/apex/log"
	"github.com/m3db/m3/src/metrics/carbon"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/uber-go/tally"
)

// Options configures the ingester.
type Options struct{}

// NewIngester returns an ingester for carbon metrics.
func NewIngester(
	ingester ingest.Ingester,
	m tally.Scope,
	opts Options,
) m3xserver.Handler {
	return &carbonHandler{
		ingester: ingester,
		metrics:  newCarbonHandlerMetrics(m),
		opts:     opts,
	}
}

func (h *carbonHandler) Handle(conn net.Conn) {
	var (
		wg                sync.WaitGroup
		beforeNetworkRead time.Time
	)

	callbackable := xserver.NewCallbackable(nil, func() { wg.Done() })
	s := carbon.NewScanner(conn)
	for {
		rolled := dice.Roll()

		// NB(cw) beforeNetworkRead must be read for every metrics now because it needs to be passed to
		// writeFn for potential per-dc metric book keeping.
		beforeNetworkRead = time.Now()

		if !s.Scan() {
			break
		}

		name, timestamp, value, recvd := s.MetricAndRecvTime()

		if rolled {
			h.metrics.readTimeLatency.Record(time.Now().Sub(beforeNetworkRead))
		}
		h.metrics.malformedCounter.Inc(int64(s.MalformedCount))
		s.MalformedCount = 0

		var policy policy.StoragePolicy
		if h.opts.EnablePolicyResolution {
			var resolved bool
			policy, resolved = h.opts.PolicyResolver.Resolve(name)
			if !resolved {
				if rolled {
					log.Warnf("unable to resolve policy for id %s", name)
				}
				h.metrics.unresolvedIDs.Inc(1)
				continue
			}
		} else {
			policy = h.opts.StaticPolicy
		}

		wg.Add(1)
		h.ingester.Ingest(name, timestamp, recvd, value, policy, protocols.Carbon, callbackable)
	}

	// Wait for all outstanding writes
	wg.Wait()
}

func (h *carbonHandler) Close() {}

func newCarbonHandlerMetrics(m tally.Scope) carbonHandlerMetrics {
	writesScope := m.SubScope("writes")
	return carbonHandlerMetrics{
		unresolvedIDs:    writesScope.Counter("ids-policy-unresolved"),
		malformedCounter: writesScope.Counter("malformed"),
		readTimeLatency:  writesScope.Timer("read-time-latency"),
	}
}

type carbonHandlerMetrics struct {
	unresolvedIDs    tally.Counter
	malformedCounter tally.Counter
	readTimeLatency  tally.Timer
}
