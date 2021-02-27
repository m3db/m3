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
	"runtime"
	"time"

	"github.com/Allenxuxu/gev"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/aggregator/aggregator"
)

const _poolRecycleInterval = 30 * time.Second

// NewServer creates a new raw TCP server.
func NewServer(address string, aggregator aggregator.Aggregator, opts Options) (*Server, error) {
	iOpts := opts.InstrumentOptions()
	handlerScope := iOpts.MetricsScope().Tagged(map[string]string{"handler": "rawtcp"})
	logger := iOpts.Logger()

	pool, err := ants.NewPool(512,
		ants.WithPanicHandler(func(v interface{}) {
			panic(v)
		}),
		ants.WithNonblocking(false),
		ants.WithLogger(poolZapLogger{logger: logger}),
		ants.WithExpiryDuration(_poolRecycleInterval),
		ants.WithPreAlloc(false),
	)

	if err != nil {
		return nil, err
	}

	keepalive := opts.ServerOptions().TCPConnectionKeepAlivePeriod()
	if !opts.ServerOptions().TCPConnectionKeepAlive() {
		keepalive = 0
	}

	return &Server{
		addr:      address,
		logger:    logger,
		keepalive: keepalive,
		handler:   NewConnHandler(aggregator, pool, logger, handlerScope, opts.ErrorLogLimitPerSecond()),
	}, nil
}

// Server is raw TCP server.
type Server struct {
	addr      string
	keepalive time.Duration
	logger    *zap.Logger
	handler   *connHandler
	srv       *gev.Server
}

// ListenAndServe starts the server and event loops.
func (s *Server) ListenAndServe() error {
	opts := []gev.Option{
		gev.NumLoops(runtime.GOMAXPROCS(0)),
		gev.Address(s.addr),
		gev.Protocol(s.handler),
		gev.Network("tcp"),
	}

	//if s.keepalive > 0 {
	//	opts = append(opts, )
	//}
	srv, err := gev.NewServer(s.handler, opts...)
	if err != nil {
		return err
	}

	s.srv = srv
	s.srv.Start()

	return nil
}

// Close closes the server.
func (s *Server) Close() error {
	if s.srv != nil {
		s.srv.Stop()
		s.handler.Close()
	}
	return nil
}
