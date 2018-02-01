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

package node

import (
	"net"
	"net/http"

	ns "github.com/m3db/m3db/network/server"
	"github.com/m3db/m3db/network/server/httpjson"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	ttnode "github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3x/context"
)

type server struct {
	address string
	db      storage.Database
	opts    httpjson.ServerOptions
	ttopts  tchannelthrift.Options
}

// NewServer creates a node HTTP network service
func NewServer(
	db storage.Database,
	address string,
	contextPool context.Pool,
	opts httpjson.ServerOptions,
	ttopts tchannelthrift.Options,
) ns.NetworkService {
	if opts == nil {
		opts = httpjson.NewServerOptions()
	}
	if ttopts == nil {
		ttopts = tchannelthrift.NewOptions()
	}
	opts = opts.
		SetContextFn(httpjson.NewDefaultContextFn(contextPool)).
		SetPostResponseFn(httpjson.DefaulPostResponseFn)
	return &server{
		address: address,
		db:      db,
		opts:    opts,
		ttopts:  ttopts,
	}
}

func (s *server) ListenAndServe() (ns.Close, error) {
	mux := http.NewServeMux()
	if err := httpjson.RegisterHandlers(mux, ttnode.NewService(s.db, s.ttopts), s.opts); err != nil {
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
	}, nil
}
