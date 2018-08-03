// Copyright (c) 2017 Uber Technologies, Inc.
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

package m3db

import (
	"fmt"
	"sync"

	m3dbrpc "github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	m3dbchannel "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node/channel"
	"github.com/m3db/m3em/node"

	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type m3emNode struct {
	sync.Mutex
	node.ServiceNode

	opts      Options
	rpcClient m3dbrpc.TChanNode
}

// New constructs a new m3emNode
func New(svcNode node.ServiceNode, opts Options) (Node, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &m3emNode{
		ServiceNode: svcNode,
		opts:        opts,
	}, nil
}

func (n *m3emNode) client() (m3dbrpc.TChanNode, error) {
	n.Lock()
	defer n.Unlock()
	if n.rpcClient != nil {
		return n.rpcClient, nil
	}
	channel, err := tchannel.NewChannel("Client", nil)
	if err != nil {
		return nil, fmt.Errorf("could not create new tchannel channel: %v", err)
	}
	endpoint := &thrift.ClientOptions{HostPort: n.Endpoint()}
	thriftClient := thrift.NewClient(channel, m3dbchannel.ChannelName, endpoint)
	client := m3dbrpc.NewTChanNodeClient(thriftClient)
	n.rpcClient = client
	return n.rpcClient, nil
}

func (n *m3emNode) Health() (NodeHealth, error) {
	healthResult := NodeHealth{}

	client, err := n.client()
	if err != nil {
		return healthResult, err
	}

	attemptFn := func() error {
		tctx, _ := thrift.NewContext(n.opts.NodeOptions().OperationTimeout())
		result, err := client.Health(tctx)
		if err != nil {
			return err
		}
		healthResult.Bootstrapped = result.GetBootstrapped()
		healthResult.OK = result.GetOk()
		healthResult.Status = result.GetStatus()
		return nil
	}

	retrier := n.opts.NodeOptions().Retrier()
	err = retrier.Attempt(attemptFn)
	return healthResult, err
}

func (n *m3emNode) Bootstrapped() bool {
	health, err := n.Health()
	if err != nil {
		return false
	}
	return health.Bootstrapped
}
