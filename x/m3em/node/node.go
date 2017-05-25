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

	"github.com/m3db/m3em/node"

	m3dbrpc "github.com/m3db/m3db/generated/thrift/rpc"
	m3dbchannel "github.com/m3db/m3db/network/server/tchannelthrift/node/channel"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

type m3dbNode struct {
	sync.Mutex
	node.ServiceNode

	opts       Options
	m3dbClient m3dbrpc.TChanNode
}

// New constructs a new M3DBNode
func New(svcNode node.ServiceNode, opts Options) (Node, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &m3dbNode{
		ServiceNode: svcNode,
		opts:        opts,
	}, nil
}

func (n *m3dbNode) thriftClient() (m3dbrpc.TChanNode, error) {
	n.Lock()
	defer n.Unlock()
	if n.m3dbClient != nil {
		return n.m3dbClient, nil
	}
	channel, err := tchannel.NewChannel("Client", nil)
	if err != nil {
		return nil, fmt.Errorf("could not create new tchannel channel: %v", err)
	}
	endpoint := &thrift.ClientOptions{HostPort: n.Endpoint()}
	thriftClient := thrift.NewClient(channel, m3dbchannel.ChannelName, endpoint)
	client := m3dbrpc.NewTChanNodeClient(thriftClient)
	n.m3dbClient = client
	return n.m3dbClient, nil
}

func (n *m3dbNode) Health() (NodeHealth, error) {
	healthResult := NodeHealth{}

	client, err := n.thriftClient()
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

func (n *m3dbNode) Bootstrapped() bool {
	health, err := n.Health()
	if err != nil {
		return false
	}
	return health.Bootstrapped
}
