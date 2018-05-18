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
	"testing"

	m3dbrpc "github.com/m3db/m3db/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3em/generated/proto/m3em"
	"github.com/m3db/m3em/node"
	mocknode "github.com/m3db/m3em/node/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func newTestOptions() Options {
	return NewOptions(nil).
		SetNodeOptions(
			node.NewOptions(nil).
				SetOperatorClientFn(func() (*grpc.ClientConn, m3em.OperatorClient, error) { return nil, nil, nil }),
		)
}

func TestHealthEndpoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := m3dbrpc.NewMockTChanNode(ctrl)
	mockClient.EXPECT().Health(gomock.Any()).Return(&m3dbrpc.NodeHealthResult_{
		Bootstrapped: true,
		Ok:           false,
		Status:       "NOT_OK",
	}, nil)

	opts := newTestOptions()
	mockNode := mocknode.NewMockServiceNode(ctrl)

	nodeInterface, err := New(mockNode, opts)
	require.NoError(t, err)
	testNode := nodeInterface.(*m3emNode)
	testNode.rpcClient = mockClient

	health, err := testNode.Health()
	require.NoError(t, err)
	require.True(t, health.Bootstrapped)
	require.False(t, health.OK)
	require.Equal(t, "NOT_OK", health.Status)
}
