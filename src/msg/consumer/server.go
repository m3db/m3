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

package consumer

import (
	"net"

	"github.com/m3db/m3x/server"
)

// NewServer creates a new server.
func NewServer(addr string, opts ServerOptions) server.Server {
	return server.NewServer(
		addr,
		NewHandler(opts.ConsumeFn(), opts.ConsumerOptions()),
		opts.ServerOptions(),
	)
}

type handler struct {
	opts      Options
	mPool     *messagePool
	consumeFn ConsumeFn

	m metrics
}

// NewHandler creates a new handler.
func NewHandler(consumeFn ConsumeFn, opts Options) server.Handler {
	mPool := newMessagePool(opts.MessagePoolOptions())
	mPool.Init()
	return &handler{
		consumeFn: consumeFn,
		opts:      opts,
		mPool:     mPool,
		m:         newConsumerMetrics(opts.InstrumentOptions().MetricsScope()),
	}
}

func (h *handler) Handle(conn net.Conn) {
	h.consumeFn(newConsumer(conn, h.mPool, h.opts, h.m))
}

func (h *handler) Close() {}
