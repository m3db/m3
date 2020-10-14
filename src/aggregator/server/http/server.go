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

package http

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	xdebug "github.com/m3db/m3/src/x/debug"
	"github.com/m3db/m3/src/x/instrument"
	xserver "github.com/m3db/m3/src/x/server"
)

const (
	defaultCPUProfileDuration = 5 * time.Second
)

// server is an http server.
type server struct {
	opts       Options
	address    string
	listener   net.Listener
	aggregator aggregator.Aggregator
	iOpts      instrument.Options
}

// NewServer creates a new http server.
func NewServer(
	address string,
	aggregator aggregator.Aggregator,
	opts Options,
	iOpts instrument.Options,
) xserver.Server {
	return &server{
		opts:       opts,
		address:    address,
		aggregator: aggregator,
		iOpts:      iOpts,
	}
}

func (s *server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	return s.Serve(listener)
}

func (s *server) Serve(l net.Listener) error {
	registerHandlers(s.opts.Mux(), s.aggregator)

	// create and register debug handler
	debugWriter, err := xdebug.NewZipWriterWithDefaultSources(
		defaultCPUProfileDuration,
		s.iOpts,
	)
	if err != nil {
		return fmt.Errorf("unable to create debug writer: %v", err)
	}

	if err := debugWriter.RegisterHandler(xdebug.DebugURL, s.opts.Mux()); err != nil {
		return fmt.Errorf("unable to register debug writer endpoint: %v", err)
	}

	server := http.Server{
		Handler:      s.opts.Mux(),
		ReadTimeout:  s.opts.ReadTimeout(),
		WriteTimeout: s.opts.WriteTimeout(),
	}

	s.listener = l
	s.address = l.Addr().String()

	go func() {
		server.Serve(l)
	}()

	return nil
}

func (s *server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.listener = nil
}
