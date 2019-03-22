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

package cluster

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/m3em/build"
	"github.com/m3db/m3/src/m3em/node"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	defaultRandSeed         = 1234567890
	defaultTestSessionToken = "someLongString"
)

var (
	defaultRandomVar = rand.New(rand.NewSource(int64(defaultRandSeed)))
)

func newDefaultClusterTestOptions(ctrl *gomock.Controller, psvc placement.Service) Options {
	mockBuild := build.NewMockServiceBuild(ctrl)
	mockConf := build.NewMockServiceConfiguration(ctrl)
	return NewOptions(psvc, nil).
		SetNumShards(10).
		SetReplication(10).
		SetServiceBuild(mockBuild).
		SetServiceConfig(mockConf).
		SetSessionToken(defaultTestSessionToken).
		SetPlacementService(psvc)
}

func newMockServiceNode(ctrl *gomock.Controller) *node.MockServiceNode {
	r := defaultRandomVar
	node := node.NewMockServiceNode(ctrl)
	node.EXPECT().ID().AnyTimes().Return(fmt.Sprintf("%d", r.Int()))
	node.EXPECT().IsolationGroup().AnyTimes().Return(fmt.Sprintf("%d", r.Int()))
	node.EXPECT().Endpoint().AnyTimes().Return(fmt.Sprintf("%v:%v", r.Int(), r.Int()))
	node.EXPECT().Zone().AnyTimes().Return(fmt.Sprintf("%d", r.Int()))
	node.EXPECT().Weight().AnyTimes().Return(uint32(r.Int()))
	node.EXPECT().Shards().AnyTimes().Return(nil)
	return node
}

type expectNodeCallTypes struct {
	expectSetup    bool
	expectTeardown bool
	expectStop     bool
	expectStart    bool
}

// nolint: unparam
func newMockServiceNodes(ctrl *gomock.Controller, numNodes int, calls expectNodeCallTypes) []node.ServiceNode {
	nodes := make([]node.ServiceNode, 0, numNodes)
	for i := 0; i < numNodes; i++ {
		mNode := newMockServiceNode(ctrl)
		if calls.expectSetup {
			mNode.EXPECT().Setup(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		}
		if calls.expectTeardown {
			mNode.EXPECT().Teardown().Return(nil)
		}
		if calls.expectStop {
			mNode.EXPECT().Stop().Return(nil)
		}
		if calls.expectStart {
			mNode.EXPECT().Start().Return(nil)
		}
		nodes = append(nodes, mNode)
	}
	return nodes
}

func newMockPlacementService(ctrl *gomock.Controller) placement.Service {
	return placement.NewMockService(ctrl)
}

func TestClusterErrorStatusTransitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPlacementService := newMockPlacementService(ctrl)
	opts := newDefaultClusterTestOptions(ctrl, mockPlacementService)
	nodes := newMockServiceNodes(ctrl, 5, expectNodeCallTypes{expectTeardown: true})
	clusterIface, err := New(nodes, opts)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())
	cluster.status = ClusterStatusError

	// illegal transitions
	_, err = cluster.Setup(1)
	require.Error(t, err)
	require.Error(t, cluster.Start())
	require.Error(t, cluster.Stop())
	_, err = cluster.AddNode()
	require.Error(t, err)
	err = cluster.RemoveNode(nil)
	require.Error(t, err)
	_, err = cluster.ReplaceNode(nil)
	require.Error(t, err)

	// teardown (legal)
	require.NoError(t, cluster.Teardown())
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())
}

func TestClusterUninitializedToSetupTransition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		mpsvc                = mockPlacementService.(*placement.MockService)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		nodes                = newMockServiceNodes(ctrl, 5, expectNodeCallTypes{expectSetup: true})
		clusterIface, err    = New(nodes, opts)
	)

	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	// fake placement
	pi, ok := nodes[0].(placement.Instance)
	require.True(t, ok)
	mockNode, ok := nodes[0].(*node.MockServiceNode)
	require.True(t, ok)
	mockNode.EXPECT().SetShards(gomock.Any())
	mockPlacement := placement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Instances().Return([]placement.Instance{pi}).AnyTimes()

	// setup (legal)
	gomock.InOrder(
		mpsvc.EXPECT().Placement().Return(nil, nil),
		mpsvc.EXPECT().Delete().Return(nil),
		mpsvc.EXPECT().
			BuildInitialPlacement(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(mockPlacement, nil),
	)

	_, err = cluster.Setup(1)
	require.NoError(t, err)
	require.Equal(t, ClusterStatusSetup, cluster.Status())
}

func TestClusterUninitializedErrorTransitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		nodes                = newMockServiceNodes(ctrl, 5, expectNodeCallTypes{})
		clusterIface, err    = New(nodes, opts)
	)

	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	// illegal transitions
	require.Error(t, cluster.Start())
	require.Error(t, cluster.Stop())
	_, err = cluster.AddNode()
	require.Error(t, err)
	err = cluster.RemoveNode(nil)
	require.Error(t, err)
	_, err = cluster.ReplaceNode(nil)
	require.Error(t, err)
}

func TestClusterSetupIllegalTransitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		nodes                = newMockServiceNodes(ctrl, 5, expectNodeCallTypes{})
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	cluster.status = ClusterStatusSetup
	require.Error(t, cluster.Stop())
}

func TestClusterSetupAddNodeTransition(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		mpsvc                = mockPlacementService.(*placement.MockService)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		expectCalls          = expectNodeCallTypes{}
		nodes                = newMockServiceNodes(ctrl, 5, expectCalls)
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	// fake placement
	pi, ok := nodes[0].(placement.Instance)
	require.True(t, ok)
	mockNode, ok := nodes[0].(*node.MockServiceNode)
	require.True(t, ok)
	mockNode.EXPECT().SetShards(gomock.Any())
	mockPlacement := placement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Instances().Return([]placement.Instance{pi}).AnyTimes()
	gomock.InOrder(
		mpsvc.EXPECT().AddInstances(gomock.Any()).Return(nil, nil, fmt.Errorf("faking error to ensure retries")),
		mpsvc.EXPECT().AddInstances(gomock.Any()).Return(mockPlacement, []placement.Instance{mockNode}, nil),
	)

	// ensure mockNode is in the spares
	found := false
	for _, inst := range cluster.SpareNodes() {
		if inst.ID() == mockNode.ID() {
			require.False(t, found)
			found = true
		}
	}
	require.True(t, found)

	// now add the mockNode using the faked stuff above
	cluster.status = ClusterStatusSetup
	newNode, err := cluster.AddNode()
	require.NoError(t, err)
	require.Equal(t, mockNode.ID(), newNode.ID())

	// ensure mockNode is not in spares
	for _, inst := range cluster.SpareNodes() {
		if inst.ID() == mockNode.ID() {
			require.Fail(t, "found node with id: %s", mockNode.ID())
		}
	}
}

func TestClusterSetupToStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		mpsvc                = mockPlacementService.(*placement.MockService)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		expectCalls          = expectNodeCallTypes{expectSetup: true}
		nodes                = newMockServiceNodes(ctrl, 5, expectCalls)
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	// fake placement
	pi, ok := nodes[0].(placement.Instance)
	require.True(t, ok)
	mockNode, ok := nodes[0].(*node.MockServiceNode)
	require.True(t, ok)
	mockNode.EXPECT().SetShards(gomock.Any())
	mockNode.EXPECT().Start().Return(nil)
	mockPlacement := placement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Instances().Return([]placement.Instance{pi}).AnyTimes()

	// setup (legal)
	gomock.InOrder(
		mpsvc.EXPECT().Placement().Return(nil, nil),
		mpsvc.EXPECT().Delete().Return(nil),
		mpsvc.EXPECT().
			BuildInitialPlacement(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(mockPlacement, nil),
	)

	_, err = cluster.Setup(1)
	require.NoError(t, err)
	require.Equal(t, ClusterStatusSetup, cluster.Status())

	// now ensure start is called
	require.NoError(t, cluster.Start())
	require.Equal(t, ClusterStatusRunning, cluster.Status())
}

func TestClusterSetupToRemoveNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		mpsvc                = mockPlacementService.(*placement.MockService)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		expectCalls          = expectNodeCallTypes{expectSetup: true}
		nodes                = newMockServiceNodes(ctrl, 5, expectCalls)
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	// fake placement
	pi, ok := nodes[0].(placement.Instance)
	require.True(t, ok)
	mockNode, ok := nodes[0].(*node.MockServiceNode)
	require.True(t, ok)
	mockNode.EXPECT().SetShards(gomock.Any())
	mockPlacement := placement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Instances().Return([]placement.Instance{pi}).AnyTimes()

	// setup (legal)
	gomock.InOrder(
		mpsvc.EXPECT().Placement().Return(nil, nil),
		mpsvc.EXPECT().Delete().Return(nil),
		mpsvc.EXPECT().
			BuildInitialPlacement(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(mockPlacement, nil),
	)

	setupNodes, err := cluster.Setup(1)
	require.NoError(t, err)
	require.Equal(t, ClusterStatusSetup, cluster.Status())
	require.Equal(t, 1, len(setupNodes))
	require.Equal(t, mockNode.ID(), setupNodes[0].ID())

	mockNode.EXPECT().SetShards(shard.NewShards(nil))
	mockPlacement = placement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Instances().Return([]placement.Instance{}).AnyTimes()
	gomock.InOrder(
		mpsvc.EXPECT().RemoveInstances([]string{setupNodes[0].ID()}).Return(nil, fmt.Errorf("faking error to ensure retries")),
		mpsvc.EXPECT().RemoveInstances([]string{setupNodes[0].ID()}).Return(mockPlacement, nil),
	)

	err = cluster.RemoveNode(setupNodes[0])
	require.NoError(t, err)

	// ensure node is in the spares list
	found := false
	for _, node := range cluster.SpareNodes() {
		if node.ID() == mockNode.ID() {
			require.False(t, found)
			found = true
		}
	}
	require.True(t, found)
}

func TestClusterSetupToReplaceNode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		mpsvc                = mockPlacementService.(*placement.MockService)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		expectCalls          = expectNodeCallTypes{expectSetup: true}
		nodes                = newMockServiceNodes(ctrl, 5, expectCalls)
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	// fake placement
	pi, ok := nodes[0].(placement.Instance)
	require.True(t, ok)
	mockNode, ok := nodes[0].(*node.MockServiceNode)
	require.True(t, ok)
	mockNode.EXPECT().SetShards(gomock.Any())
	mockPlacement := placement.NewMockPlacement(ctrl)
	mockPlacement.EXPECT().Instances().Return([]placement.Instance{pi}).AnyTimes()

	// setup (legal)
	gomock.InOrder(
		mpsvc.EXPECT().Placement().Return(nil, nil),
		mpsvc.EXPECT().Delete().Return(nil),
		mpsvc.EXPECT().
			BuildInitialPlacement(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(mockPlacement, nil),
	)

	setupNodes, err := cluster.Setup(1)
	require.NoError(t, err)
	require.Equal(t, ClusterStatusSetup, cluster.Status())
	require.Equal(t, 1, len(setupNodes))
	require.Equal(t, mockNode.ID(), setupNodes[0].ID())

	// create new mock placement for replace
	mockNode.EXPECT().SetShards(shard.NewShards(nil))
	mockPlacement = placement.NewMockPlacement(ctrl)
	replacementInstances := []placement.Instance{
		nodes[1].(placement.Instance),
		nodes[2].(placement.Instance),
	}
	mockPlacement.EXPECT().Instances().Return(replacementInstances).AnyTimes()
	mockNode1 := nodes[1].(*node.MockServiceNode)
	mockNode2 := nodes[2].(*node.MockServiceNode)
	mockNode1.EXPECT().SetShards(gomock.Any())
	mockNode2.EXPECT().SetShards(gomock.Any())

	gomock.InOrder(
		mpsvc.EXPECT().
			ReplaceInstances([]string{setupNodes[0].ID()}, gomock.Any()).
			Return(nil, nil, fmt.Errorf("faking error to ensure retries")),
		mpsvc.EXPECT().
			ReplaceInstances([]string{setupNodes[0].ID()}, gomock.Any()).
			Return(mockPlacement, replacementInstances, nil),
	)

	replacementNodes, err := cluster.ReplaceNode(setupNodes[0])
	require.NoError(t, err)
	require.Equal(t, 2, len(replacementNodes))
}

func TestClusterRunningIllegalTransitions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		nodes                = newMockServiceNodes(ctrl, 5, expectNodeCallTypes{})
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	cluster.status = ClusterStatusRunning
	require.Error(t, cluster.Start())
	_, err = cluster.Setup(1)
	require.Error(t, err)
}

func TestClusterRunningToStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		nodes                = newMockServiceNodes(ctrl, 5, expectNodeCallTypes{})
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	cluster.status = ClusterStatusRunning
	mockNode, ok := nodes[0].(*node.MockServiceNode)
	require.True(t, ok)
	mockNode.EXPECT().Stop().Return(nil)
	usedIDMap := map[string]node.ServiceNode{
		mockNode.ID(): mockNode,
	}
	cluster.usedNodes = usedIDMap

	require.NoError(t, cluster.Stop())
}

func TestClusterRunningToTeardown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var (
		mockPlacementService = newMockPlacementService(ctrl)
		opts                 = newDefaultClusterTestOptions(ctrl, mockPlacementService)
		nodes                = newMockServiceNodes(ctrl, 5, expectNodeCallTypes{expectTeardown: true})
		clusterIface, err    = New(nodes, opts)
	)
	require.NoError(t, err)
	cluster := clusterIface.(*svcCluster)
	require.Equal(t, ClusterStatusUninitialized, cluster.Status())

	cluster.status = ClusterStatusRunning
	require.NoError(t, cluster.Teardown())
}
