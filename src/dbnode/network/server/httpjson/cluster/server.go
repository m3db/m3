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

package cluster

import (
	"net"
	"net/http"

	"github.com/m3db/m3/src/dbnode/client"
	ns "github.com/m3db/m3/src/dbnode/network/server"
	"github.com/m3db/m3/src/dbnode/network/server/httpjson"
	ttcluster "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/cluster"
	"github.com/m3db/m3/src/x/context"
	xresource "github.com/m3db/m3/src/x/resource"
)

type server struct {
	client  client.Client
	address string
	opts    httpjson.ServerOptions
}

// NewServer creates a cluster HTTP network service.
func NewServer(
	client client.Client,
	address string,
	contextPool context.Pool,
	opts httpjson.ServerOptions,
) ns.NetworkService {
	if opts == nil {
		opts = httpjson.NewServerOptions()
	}
	opts = opts.
		SetContextFn(httpjson.NewDefaultContextFn(contextPool)).
		SetPostResponseFn(httpjson.DefaulPostResponseFn)
	return &server{
		client:  client,
		address: address,
		opts:    opts,
	}
}

func (s *server) ListenAndServe() (ns.Close, error) {
	service := ttcluster.NewService(s.client)

	mux := http.NewServeMux()
	if err := httpjson.RegisterHandlers(mux, service, s.opts); err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return nil, err
	}

	server := http.Server{
		Handler:      mux,
		ReadTimeout:  s.opts.ReadTimeout(),
		WriteTimeout: s.opts.WriteTimeout(),
	}

	go func() {
		server.Serve(listener)
	}()

	return func() {
		listener.Close()
		xresource.TryClose(service) // nolint: errcheck
	}, nil
}
