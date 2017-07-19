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
	"time"

	"github.com/m3db/m3em/build"
	"github.com/m3db/m3em/node"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

// Status indicates the different states a Cluster can be in. Refer to the
// state diagram below to understand transitions. Note, all states can transition
// to `ClusterStatusError`, these edges are skipped below.
//              ╔══════════════════╗
//              ║                  ║
//              ║  Uninitialized   ║────────────Setup()────┐
//              ║                  ║                       │
//              ╚══════════════════╝                       │
//                        ▲                                │
//                        │                                │
//                        │                                │
//                        │   Teardown()                   │
//                        ├────────────────────────┐       │
//                        │                        │       │
//                ┌───────┘                        │       │
//                │                                │       ▼
//          ╔═════╩════════════╗◀────Start()─────╦═╩════════════════╗
//          ║                  ║                 ║                  ║
//       ┌──║     Running      ║                 ║      Setup       ║──┐
//       │  ║                  ║                 ║                  ║  │
//       │  ╚══════════════════╝──────Stop()────▶╚══════════════════╝  │
// AddInstance()      ▲                                    ▲    AddInstance()
// RemoveInstance()   │                                    │    RemoveInstance()
// ReplaceInstance()──┘                                    └────ReplaceInstance()
type Status int

const (
	// ClusterStatusUninitialized refers to the state of an un-initialized cluster.
	ClusterStatusUninitialized Status = iota

	// ClusterStatusSetup refers to the state of a cluster whose nodes have
	// been setup, and the cluster has an assigned placement.
	ClusterStatusSetup

	// ClusterStatusRunning refers to the state of a cluster with running nodes.
	// There is no restriction on the number of nodes running, or assigned within
	// the cluster placement.
	ClusterStatusRunning

	// ClusterStatusError refers to a cluster in error.
	ClusterStatusError
)

// Cluster is a collection of ServiceNodes with a m3cluster Placement
type Cluster interface {
	// Setup sets up the service placement for the specified numNodes.
	Setup(numNodes int) ([]node.ServiceNode, error)

	// Teardown releases the resources acquired during Setup().
	Teardown() error

	// AddNode adds a node to the service placement. The node to add is chosen from
	// amongst // available spares, by the placementservice. It does NOT alter the
	// state of the ServiceNode (i.e. does not start/stop it).
	AddNode() (node.ServiceNode, error)

	// AddSpecifiedNode adds the specified node to the service placement. It does
	// NOT alter the state of the ServiceNode (i.e. does not start/stop it).
	AddSpecifiedNode(node.ServiceNode) error

	// RemoveNode removes the specified node from the service placement. It does
	// NOT alter the state of the ServiceNode (i.e. does not start/stop it).
	RemoveNode(node.ServiceNode) error

	// ReplaceNode replaces the specified node with new node(s) in the service
	// placement. It does NOT alter the state of the ServiceNode (i.e. does not start/stop it).
	ReplaceNode(oldNode node.ServiceNode) ([]node.ServiceNode, error)

	// Start starts all nodes used in current service placement.
	Start() error

	// Stop stops all nodes used in current service placement.
	Stop() error

	// SpareNodes returns the list of known nodes which are not part of the defined placement
	// in the cluster.
	SpareNodes() []node.ServiceNode

	// ActiveNodes returns the list of known nodes which are part of the defined placement
	// in the cluster.
	ActiveNodes() []node.ServiceNode

	// KnownNodes returns the list of known nodes
	KnownNodes() []node.ServiceNode

	// Placement returns the current placement
	Placement() services.Placement

	// Status returns the cluster status
	Status() Status
}

// Options represents the options to configure a `Cluster`
type Options interface {
	// Validate validates the Options
	Validate() error

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetServiceBuild sets the service build used to configure
	// the cluster
	SetServiceBuild(build.ServiceBuild) Options

	// ServiceBuild returns the service build used in the cluster
	ServiceBuild() build.ServiceBuild

	// SetServiceConfig sets the default service configuration to
	// be used in setting up the cluster
	SetServiceConfig(build.ServiceConfiguration) Options

	// ServiceConfig returns the default service configuration to
	// used in setting up the cluster
	ServiceConfig() build.ServiceConfiguration

	// SetSessionToken sets the token used for interactions with remote m3em agents
	SetSessionToken(string) Options

	// SessionToken returns the token used for interactions with remote m3em agents
	SessionToken() string

	// SetSessionOverride sets a flag indicating if m3em agent operations
	// are permitted to override clashing resources
	SetSessionOverride(bool) Options

	// SessionOverride returns a flag indicating if m3em agent operations
	// are permitted to override clashing resources
	SessionOverride() bool

	// SetReplication sets the replication factor for the cluster
	SetReplication(int) Options

	// Replication returns the replication factor by the cluster
	Replication() int

	// SetNumShards sets the number of shards used in the cluster
	SetNumShards(int) Options

	// NumShards returns the number of shards used in the cluster
	NumShards() int

	// SetPlacementService returns the PlacementService to use for cluster
	// configuration
	SetPlacementService(services.PlacementService) Options

	// PlacementService returns the PlacementService to use for cluster
	// configuration
	PlacementService() services.PlacementService

	// SetPlacementServiceRetrier sets the retrier to use for placement
	// service operations
	SetPlacementServiceRetrier(xretry.Retrier) Options

	// PlacementServiceRetrier returns the retrier to use for placement
	// service operations
	PlacementServiceRetrier() xretry.Retrier

	// SetNodeConcurrency sets the number of nodes to operate upon
	// concurrently
	SetNodeConcurrency(int) Options

	// NodeConcurrency returns the number of nodes to operate upon
	// concurrently
	NodeConcurrency() int

	// SetNodeOperationTimeout sets the timeout for a node operation
	SetNodeOperationTimeout(time.Duration) Options

	// NodeOperationTimeout returns the timeout for a node operation
	NodeOperationTimeout() time.Duration

	// SetNodeListener sets default node listener
	SetNodeListener(node.Listener) Options

	// NodeListener returns the node listener
	NodeListener() node.Listener
}
