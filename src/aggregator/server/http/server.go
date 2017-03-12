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
	"net"
	"net/http"

	"github.com/m3db/m3aggregator/aggregator"
	networkserver "github.com/m3db/m3aggregator/server"
)

// server is an http server receiving incoming metrics traffic
type server struct {
	opts       Options
	address    string
	listener   net.Listener
	aggregator aggregator.Aggregator
}

// NewServer creates a new http server
func NewServer(address string, aggregator aggregator.Aggregator, opts Options) networkserver.Server {
	return &server{
		opts:       opts,
		address:    address,
		aggregator: aggregator,
	}
}

func (s *server) ListenAndServe() error {
	mux := http.NewServeMux()
	if err := registerHandlers(mux, s.aggregator); err != nil {
		return err
	}
	server := http.Server{
		Handler:      mux,
		ReadTimeout:  s.opts.ReadTimeout(),
		WriteTimeout: s.opts.WriteTimeout(),
	}

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener

	go func() {
		server.Serve(listener)
	}()

	return nil
}

func (s *server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}
