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
	ns "github.com/m3db/m3/src/dbnode/network/server"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node/channel"
	"github.com/m3db/m3/src/x/context"

	"github.com/uber/tchannel-go"
)

type server struct {
	service     Service
	address     string
	contextPool context.Pool
	opts        Options
}

// NewServer creates a new node TChannel Thrift network service
func NewServer(
	service Service,
	address string,
	contextPool context.Pool,
	opts Options,
) ns.NetworkService {
	return &server{
		service:     service,
		address:     address,
		contextPool: contextPool,
		opts:        opts,
	}
}

func (s *server) ListenAndServe() (ns.Close, error) {
	// NB(r): Keep ref to a local channel options since it's actually modified
	// by TChannel itself to set defaults.
	var opts *tchannel.ChannelOptions
	if chanOpts := s.opts.ChannelOptions(); chanOpts != nil {
		immutableOpts := *chanOpts
		opts = &immutableOpts
	}
	channel, err := s.opts.TChanChannelFn()(s.service, channel.ChannelName, opts)
	if err != nil {
		return nil, err
	}

	iOpts := s.opts.InstrumentOptions()
	server := s.opts.TChanNodeServerFn()(s.service, iOpts)
	tchannelthrift.RegisterServer(channel, server, s.contextPool)
	channel.ListenAndServe(s.address)

	return channel.Close, nil
}
