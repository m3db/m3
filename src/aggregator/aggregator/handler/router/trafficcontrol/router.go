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

package trafficcontrol

import (
	"github.com/m3db/m3/src/aggregator/aggregator/handler/common"
	"github.com/m3db/m3/src/aggregator/aggregator/handler/router"

	"github.com/uber-go/tally"
)

type metrics struct {
	trafficControlNotAllowed tally.Counter
}

func newMetrics(scope tally.Scope) metrics {
	return metrics{
		trafficControlNotAllowed: scope.Counter("traffic-control-not-allowed"),
	}
}

type controlledRouter struct {
	Controller
	router.Router

	m metrics
}

// NewRouter creates a traffic controlled router.
func NewRouter(
	trafficController Controller,
	router router.Router,
	scope tally.Scope,
) router.Router {
	return &controlledRouter{
		Controller: trafficController,
		Router:     router,
		m:          newMetrics(scope),
	}
}

func (r *controlledRouter) Route(shard uint32, buffer *common.RefCountedBuffer) error {
	if !r.Controller.Allow() {
		buffer.DecRef()
		r.m.trafficControlNotAllowed.Inc(1)
		return nil
	}
	return r.Router.Route(shard, buffer)
}

func (r *controlledRouter) Close() {
	r.Controller.Close()
	r.Router.Close()
}
