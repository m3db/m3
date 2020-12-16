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
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	ns "github.com/m3db/m3/src/dbnode/network/server"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	"github.com/m3db/m3/src/x/context"
	xresource "github.com/m3db/m3/src/x/resource"

	"github.com/uber/tchannel-go"
)

const (
	// ChannelName is the TChannel channel name the cluster service is exposed on
	ChannelName = "Cluster"
)

type server struct {
	client      client.Client
	address     string
	contextPool context.Pool
	opts        *tchannel.ChannelOptions
}

// NewServer creates a new cluster TChannel Thrift network service
func NewServer(
	client client.Client,
	address string,
	contextPool context.Pool,
	opts *tchannel.ChannelOptions,
) ns.NetworkService {
	return &server{
		address:     address,
		client:      client,
		contextPool: contextPool,
		opts:        opts,
	}
}

func (s *server) ListenAndServe() (ns.Close, error) {
	// NB(r): Keep ref to a local channel options since it's actually modified
	// by TChannel itself to set defaults.
	var opts *tchannel.ChannelOptions
	if s.opts != nil {
		immutableOpts := *s.opts
		opts = &immutableOpts
	}
	channel, err := tchannel.NewChannel(ChannelName, opts)
	if err != nil {
		return nil, err
	}

	service := NewService(s.client)
	tchannelthrift.RegisterServer(channel, rpc.NewTChanClusterServer(service), s.contextPool)

	channel.ListenAndServe(s.address)

	return func() {
		channel.Close()
		xresource.TryClose(service) // nolint: errcheck
	}, nil
}
