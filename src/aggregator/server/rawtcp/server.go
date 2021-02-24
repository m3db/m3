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
	"context"
	"runtime"
	"time"

	"github.com/panjf2000/gnet"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/aggregator/aggregator"
)

//const _poolRecycleInterval = 5 * time.Second

// NewServer creates a new raw TCP server.
func NewServer(address string, aggregator aggregator.Aggregator, opts Options) (*Server, error) {
	iOpts := opts.InstrumentOptions()
	handlerScope := iOpts.MetricsScope().Tagged(map[string]string{"handler": "rawtcp"})
	logger := iOpts.Logger()

	keepalive := opts.ServerOptions().TCPConnectionKeepAlivePeriod()
	if !opts.ServerOptions().TCPConnectionKeepAlive() {
		keepalive = 0
	}

	return &Server{
		addr:      "tcp://" + address,
		logger:    logger,
		keepalive: keepalive,
		bufSize:   opts.ReadBufferSize(),
		handler:   NewConnHandler(aggregator, nil, logger, handlerScope, opts.ErrorLogLimitPerSecond()),
	}, nil
}

// Server is raw TCP server.
type Server struct {
	addr      string
	keepalive time.Duration
	logger    *zap.Logger
	handler   *connHandler
	bufSize   int
}

// ListenAndServe starts the server and event loops.
func (s *Server) ListenAndServe() error {
	gnet.Serve(s.handler, s.addr, gnet.WithOptions(
		gnet.Options{
			ReadBufferCap: s.bufSize,
			TCPKeepAlive:  s.keepalive,
			NumEventLoop:  runtime.GOMAXPROCS(0) / 2,
			Codec:         s.handler,
		},
	))
	return nil
}

// Close closes the server.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	s.handler.Close()
	return gnet.Stop(ctx, s.addr)
}
