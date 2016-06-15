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

package tchannelthrift

import (
	"github.com/m3db/m3db/services/m3dbnode/serve"
	"github.com/m3db/m3db/services/m3dbnode/serve/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3db/storage"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const (
	// ChannelName is the name of the TChannel channel name the service is exposed on
	ChannelName = "Node"
)

type server struct {
	address string
	opts    *tchannel.ChannelOptions
	db      storage.Database
}

// NewServer creates a TChannel Thrift network service
func NewServer(
	db storage.Database,
	address string,
	opts *tchannel.ChannelOptions,
) serve.NetworkService {
	// Make the opts immutable on the way in
	if opts != nil {
		immutableOpts := *opts
		opts = &immutableOpts
	}
	return &server{
		address: address,
		opts:    opts,
		db:      db,
	}
}

func (s *server) ListenAndServe() (serve.Close, error) {
	channel, err := tchannel.NewChannel(ChannelName, s.opts)
	if err != nil {
		return nil, err
	}

	server := thrift.NewServer(channel)
	server.Register(rpc.NewTChanNodeServer(NewService(s.db)))

	channel.ListenAndServe(s.address)

	return channel.Close, nil
}
