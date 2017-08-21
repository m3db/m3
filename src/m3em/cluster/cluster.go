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
	"sync"

	"github.com/m3db/m3em/node"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
)

var (
	errInsufficientCapacity          = fmt.Errorf("insufficient node capacity in environment")
	errNodeNotInUse                  = fmt.Errorf("unable to remove node, not in use")
	errClusterNotUnitialized         = fmt.Errorf("unable to setup cluster, it is not unitialized")
	errClusterUnableToAlterPlacement = fmt.Errorf("unable to alter cluster placement, it needs to be setup/running")
	errUnableToStartUnsetupCluster   = fmt.Errorf("unable to start cluster, it has not been setup")
	errClusterUnableToTeardown       = fmt.Errorf("unable to teardown cluster, it has not been setup")
	errUnableToStopNotRunningCluster = fmt.Errorf("unable to stop cluster, it is running")
)

type idToNodeMap map[string]node.ServiceNode

func (im idToNodeMap) values() []node.ServiceNode {
	returnNodes := make([]node.ServiceNode, 0, len(im))
	for _, node := range im {
		returnNodes = append(returnNodes, node)
	}
	return returnNodes
}

type svcCluster struct {
	sync.RWMutex

	logger       xlog.Logger
	opts         Options
	knownNodes   node.ServiceNodes
	usedNodes    idToNodeMap
	spares       []node.ServiceNode
	sparesByID   map[string]node.ServiceNode
	placementSvc placement.Service
	placement    placement.Placement
	status       Status
	lastErr      error
}

// New returns a new cluster backed by provided service nodes
func New(
	nodes node.ServiceNodes,
	opts Options,
) (Cluster, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	cluster := &svcCluster{
		logger:       opts.InstrumentOptions().Logger(),
		opts:         opts,
		knownNodes:   nodes,
		usedNodes:    make(idToNodeMap, len(nodes)),
		spares:       make([]node.ServiceNode, 0, len(nodes)),
		sparesByID:   make(map[string]node.ServiceNode, len(nodes)),
		placementSvc: opts.PlacementService(),
		status:       ClusterStatusUninitialized,
	}
	cluster.addSparesWithLock(nodes)

	return cluster, nil
}

func (c *svcCluster) addSparesWithLock(spares []node.ServiceNode) {
	for _, spare := range spares {
		c.spares = append(c.spares, spare)
		c.sparesByID[spare.ID()] = spare
	}
}

func nodeSliceWithoutID(originalSlice node.ServiceNodes, removeID string) node.ServiceNodes {
	newSlice := make(node.ServiceNodes, 0, len(originalSlice))
	for _, elem := range originalSlice {
		if elem.ID() != removeID {
			newSlice = append(newSlice, elem)
		}
	}
	return newSlice
}

func (c *svcCluster) newExecutor(
	nodes node.ServiceNodes,
	fn node.ServiceNodeFn,
) node.ConcurrentExecutor {
	return node.NewConcurrentExecutor(nodes, c.opts.NodeConcurrency(), c.opts.NodeOperationTimeout(), fn)
}

func (c *svcCluster) Placement() placement.Placement {
	c.Lock()
	defer c.Unlock()
	return c.placement
}

func (c *svcCluster) initWithLock() error {
	psvc := c.placementSvc
	_, _, err := psvc.Placement()
	if err != nil { // attempt to retrieve current placement
		c.logger.Infof("unable to retrieve existing placement, skipping delete attempt")
	} else {
		// delete existing placement
		err = c.opts.PlacementServiceRetrier().Attempt(psvc.Delete)
		if err != nil {
			return fmt.Errorf("unable to delete existing placement during setup(): %+v", err)
		}
		c.logger.Infof("successfully deleted existing placement")
	}

	var (
		svcBuild        = c.opts.ServiceBuild()
		svcConf         = c.opts.ServiceConfig()
		sessionToken    = c.opts.SessionToken()
		sessionOverride = c.opts.SessionOverride()
		listener        = c.opts.NodeListener()
	)

	// setup all known service nodes with build, config
	executor := c.newExecutor(c.knownNodes, func(node node.ServiceNode) error {
		err := node.Setup(svcBuild, svcConf, sessionToken, sessionOverride)
		if err != nil {
			return err
		}
		if listener != nil {
			// NB: no need to track returned listenerID here, it's cleaned up in node.Teardown()
			node.RegisterListener(listener)
		}
		return nil
	})
	return executor.Run()
}

func (c *svcCluster) Setup(numNodes int) ([]node.ServiceNode, error) {
	c.Lock()
	defer c.Unlock()

	if c.status != ClusterStatusUninitialized {
		return nil, errClusterNotUnitialized
	}

	numSpares := len(c.spares)
	if numSpares < numNodes {
		return nil, errInsufficientCapacity
	}

	if err := c.initWithLock(); err != nil {
		return nil, err
	}

	psvc := c.placementSvc
	spares := c.sparesAsPlacementInstaceWithLock()[:numNodes]

	// we don't need to use the retrier here as there are no other users of this placement yet
	placement, err := psvc.BuildInitialPlacement(spares, c.opts.NumShards(), c.opts.Replication())
	if err != nil {
		return nil, err
	}

	// update ServiceNode with new shards from placement
	var (
		multiErr      xerrors.MultiError
		usedInstances = placement.Instances()
		setupNodes    = make([]node.ServiceNode, 0, len(usedInstances))
	)
	for _, instance := range usedInstances {
		setupNode, err := c.markSpareUsedWithLock(instance)
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		setupNodes = append(setupNodes, setupNode)
	}

	multiErr = multiErr.
		Add(c.setPlacementWithLock(placement))

	return setupNodes, c.markStatusWithLock(ClusterStatusSetup, multiErr.FinalError())
}

func (c *svcCluster) markSpareUsedWithLock(spare placement.Instance) (node.ServiceNode, error) {
	id := spare.ID()
	spareNode, ok := c.sparesByID[id]
	if !ok {
		// should never happen
		return nil, fmt.Errorf("unable to find spare node with id: %s", id)
	}
	delete(c.sparesByID, id)
	c.spares = nodeSliceWithoutID(c.spares, id)
	c.usedNodes[id] = spareNode
	return spareNode, nil
}

func (c *svcCluster) AddSpecifiedNode(newNode node.ServiceNode) error {
	c.Lock()
	defer c.Unlock()

	if !c.isSpareNodeWithLock(newNode) {
		return fmt.Errorf("provided node is not a known spare")
	}

	_, err := c.addNodeFromListWithLock([]placement.Instance{newNode.(placement.Instance)})
	return err
}

func (c *svcCluster) isSpareNodeWithLock(n node.ServiceNode) bool {
	_, ok := c.sparesByID[n.ID()]
	return ok
}

func (c *svcCluster) addNodeFromListWithLock(candidates []placement.Instance) (node.ServiceNode, error) {
	if c.status != ClusterStatusRunning && c.status != ClusterStatusSetup {
		return nil, errClusterUnableToAlterPlacement
	}

	var (
		psvc          = c.placementSvc
		newPlacement  placement.Placement
		usedInstances []placement.Instance
	)
	if err := c.opts.PlacementServiceRetrier().Attempt(func() error {
		var internalErr error
		newPlacement, usedInstances, internalErr = psvc.AddInstances(candidates)
		return internalErr
	}); err != nil {
		return nil, err
	}

	if len(usedInstances) != 1 {
		return nil, fmt.Errorf("%d instances added to the placement, expecting 1", len(usedInstances))
	}

	setupNode, err := c.markSpareUsedWithLock(usedInstances[0])
	if err != nil {
		return nil, err
	}

	return setupNode, c.setPlacementWithLock(newPlacement)
}

func (c *svcCluster) AddNode() (node.ServiceNode, error) {
	c.Lock()
	defer c.Unlock()

	numSpares := len(c.spares)
	if numSpares < 1 {
		return nil, errInsufficientCapacity
	}

	return c.addNodeFromListWithLock(c.sparesAsPlacementInstaceWithLock())
}

func (c *svcCluster) setPlacementWithLock(p placement.Placement) error {
	for _, instance := range p.Instances() {
		// nb(prateek): update usedNodes with the new shards.
		instanceID := instance.ID()
		usedNode, ok := c.usedNodes[instanceID]
		if ok {
			usedNode.SetShards(instance.Shards())
		}
	}

	c.placement = p
	return nil
}

func (c *svcCluster) sparesAsPlacementInstaceWithLock() []placement.Instance {
	spares := make([]placement.Instance, 0, len(c.spares))
	for _, spare := range c.spares {
		spares = append(spares, spare.(placement.Instance))
	}
	return spares
}

func (c *svcCluster) RemoveNode(i node.ServiceNode) error {
	c.Lock()
	defer c.Unlock()

	if c.status != ClusterStatusRunning && c.status != ClusterStatusSetup {
		return errClusterUnableToAlterPlacement
	}

	usedNode, ok := c.usedNodes[i.ID()]
	if !ok {
		return errNodeNotInUse
	}

	var (
		newPlacement placement.Placement
		psvc         = c.placementSvc
	)
	if err := c.opts.PlacementServiceRetrier().Attempt(func() error {
		var internalErr error
		newPlacement, internalErr = psvc.RemoveInstances([]string{i.ID()})
		return internalErr
	}); err != nil {
		return err
	}

	// update removed instance from used -> spare
	// nb(prateek): this omits modeling "leaving" shards on the node being removed
	usedNode.SetShards(shard.NewShards(nil))
	delete(c.usedNodes, usedNode.ID())
	c.addSparesWithLock([]node.ServiceNode{usedNode})

	return c.setPlacementWithLock(newPlacement)
}

func (c *svcCluster) ReplaceNode(oldNode node.ServiceNode) ([]node.ServiceNode, error) {
	c.Lock()
	defer c.Unlock()

	if c.status != ClusterStatusRunning && c.status != ClusterStatusSetup {
		return nil, errClusterUnableToAlterPlacement
	}

	if _, ok := c.usedNodes[oldNode.ID()]; !ok {
		return nil, errNodeNotInUse
	}

	var (
		psvc            = c.placementSvc
		spareCandidates = c.sparesAsPlacementInstaceWithLock()
		newPlacement    placement.Placement
		newInstances    []placement.Instance
	)
	if err := c.opts.PlacementServiceRetrier().Attempt(func() error {
		var internalErr error
		newPlacement, newInstances, internalErr = psvc.ReplaceInstances([]string{oldNode.ID()}, spareCandidates)
		return internalErr
	}); err != nil {
		return nil, err
	}

	// mark old node no longer used
	oldNode.SetShards(shard.NewShards(nil))
	delete(c.usedNodes, oldNode.ID())
	c.addSparesWithLock([]node.ServiceNode{oldNode})

	var (
		multiErr xerrors.MultiError
		newNodes = make([]node.ServiceNode, 0, len(newInstances))
	)
	for _, instance := range newInstances {
		newNode, err := c.markSpareUsedWithLock(instance)
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		newNodes = append(newNodes, newNode)
	}

	multiErr = multiErr.
		Add(c.setPlacementWithLock(newPlacement))

	return newNodes, multiErr.FinalError()
}

func (c *svcCluster) SpareNodes() []node.ServiceNode {
	c.Lock()
	defer c.Unlock()
	return c.spares
}

func (c *svcCluster) ActiveNodes() []node.ServiceNode {
	c.Lock()
	defer c.Unlock()
	return c.usedNodes.values()
}

func (c *svcCluster) KnownNodes() []node.ServiceNode {
	c.Lock()
	defer c.Unlock()
	return c.knownNodes
}

func (c *svcCluster) markStatusWithLock(status Status, err error) error {
	if err == nil {
		c.status = status
		return nil
	}

	c.status = ClusterStatusError
	c.lastErr = err
	return err
}

func (c *svcCluster) Teardown() error {
	c.Lock()
	defer c.Unlock()

	if c.status == ClusterStatusUninitialized {
		return errClusterUnableToTeardown
	}

	err := c.newExecutor(c.knownNodes, func(node node.ServiceNode) error {
		return node.Teardown()
	}).Run()

	for id, usedNode := range c.usedNodes {
		usedNode.SetShards(shard.NewShards(nil))
		delete(c.usedNodes, id)
	}
	c.spares = make([]node.ServiceNode, 0, len(c.knownNodes))
	c.sparesByID = make(map[string]node.ServiceNode, len(c.knownNodes))
	c.addSparesWithLock(c.knownNodes)

	return c.markStatusWithLock(ClusterStatusUninitialized, err)
}

func (c *svcCluster) Start() error {
	c.Lock()
	defer c.Unlock()

	if c.status != ClusterStatusSetup {
		return errUnableToStartUnsetupCluster
	}

	err := c.newExecutor(c.usedNodes.values(), func(node node.ServiceNode) error {
		return node.Start()
	}).Run()

	return c.markStatusWithLock(ClusterStatusRunning, err)
}

func (c *svcCluster) Stop() error {
	c.Lock()
	defer c.Unlock()

	if c.status != ClusterStatusRunning {
		return errUnableToStopNotRunningCluster
	}

	err := c.newExecutor(c.usedNodes.values(), func(node node.ServiceNode) error {
		return node.Stop()
	}).Run()

	return c.markStatusWithLock(ClusterStatusSetup, err)
}

func (c *svcCluster) Status() Status {
	c.RLock()
	defer c.RUnlock()
	return c.status
}
