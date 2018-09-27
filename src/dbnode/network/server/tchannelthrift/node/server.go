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
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	ns "github.com/m3db/m3/src/dbnode/network/server"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node/channel"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3x/context"

	"github.com/uber/tchannel-go"
)

type server struct {
	db          storage.Database
	address     string
	contextPool context.Pool
	opts        *tchannel.ChannelOptions
	ttopts      tchannelthrift.Options
}

// NewServer creates a new node TChannel Thrift network service
func NewServer(
	db storage.Database,
	address string,
	contextPool context.Pool,
	opts *tchannel.ChannelOptions,
	ttopts tchannelthrift.Options,
) ns.NetworkService {
	// Make the opts immutable on the way in
	if opts != nil {
		immutableOpts := *opts
		opts = &immutableOpts
	}
	if ttopts == nil {
		ttopts = tchannelthrift.NewOptions()
	}
	return &server{
		db:          db,
		address:     address,
		contextPool: contextPool,
		opts:        opts,
		ttopts:      ttopts,
	}
}

func (s *server) ListenAndServe() (ns.Close, error) {
	channel, err := tchannel.NewChannel(channel.ChannelName, s.opts)
	if err != nil {
		return nil, err
	}

	service := NewService(s.db, s.ttopts)
	tchannelthrift.RegisterServer(channel, rpc.NewTChanNodeServer(service), s.contextPool, s.ttopts.ProtocolPool())

	channel.ListenAndServe(s.address)

	return channel.Close, nil
}
