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

package node

import (
	"time"

	"github.com/m3db/m3em/build"
	hb "github.com/m3db/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3em/generated/proto/m3em"

	"github.com/m3db/m3cluster/services"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
	"google.golang.org/grpc"
)

// Status indicates the different states a ServiceNode can be in. The
// state diagram below describes the transitions between the various states:
//
//                           ┌──────────────────┐
//                           │                  │
//           ┌Teardown()─────│      Error       │
//           │               │                  │
//           │               └──────────────────┘
//           │
//           ▼
// ┌──────────────────┐                         ┌───────────────-──┐
// │                  │      Setup()            │                  │
// │  Uninitialized   ├────────────────────────▶│      Setup       │◀─┐
// │                  │◀───────────┐            │                  │  │
// └──────────────────┘  Teardown()└────────────└──────────────────┘  │
//           ▲                                            │           │
//           │                                            │           │
//           │                                            │           │
//           │                                  Start()   │           │
//           │                              ┌─────────────┘           │
//           │                              │                         │
//           │                              │                         │
//           │                              │                         │
//           │                              ▼                         │
//           │                    ┌──────────────────┐                │
//           │Teardown()          │                  │                |
//           └────────────────────│     Running      │────────────Stop()
//                                │                  │
//                                └──────────────────┘
type Status int

const (
	// StatusUninitialized refers to the state of an un-initialized node.
	StatusUninitialized Status = iota

	// StatusSetup is the state of a node which has been Setup()
	StatusSetup

	// StatusRunning is the state of a node which has been Start()-ed
	StatusRunning

	// StatusError is the state of a node which is in an Error state
	StatusError
)

// ServiceNode represents an executable service node. This object controls both the service
// and resources on the host running the service (e.g. fs, processes, etc.)
type ServiceNode interface {
	services.PlacementInstance

	// Setup initializes the directories, config file, and binary for the process being tested.
	// It does not Start the process on the ServiceNode.
	Setup(
		build build.ServiceBuild,
		config build.ServiceConfiguration,
		token string,
		force bool,
	) error

	// Start starts the service process for this ServiceNode.
	Start() error

	// Stop stops the service process for this ServiceNode.
	Stop() error

	// Status returns the ServiceNode status.
	Status() Status

	// Teardown releases any remote resources used for testing.
	Teardown() error

	// Close releases any locally held resources
	Close() error

	// RegisterListener registers an event listener
	RegisterListener(Listener) ListenerID

	// DeregisterListener un-registers an event listener
	DeregisterListener(ListenerID)

	// TransferLocalFile transfers a local file to the specified destination paths
	// NB: destPaths are not allowed to use relative path specifiers, i.e. '..' is illegal;
	// the eventual destination path on remote hosts is relative to the working directory
	// of the remote agent.
	// e.g. if the remote agent has working directory /var/m3em-agent, and we make the call:
	// svcNode.TransferLocalFile("some/local/file/path/id", []string{"path/id", "another/path/id"})
	//
	// upon success, there will be two new files under the remote agent working directory
	//
	// /var/m3em-agent/
	// /var/m3em-agent/path/id          <-- same contents as "some/local/file/path/id"
	// /var/m3em-agent/another/path/id  <-- same contents as "some/local/file/path/id"
	TransferLocalFile(localSrc string, destPaths []string, overwrite bool) error

	// TODO(prateek): add operator operations for -
	// CleanDataDirectory() error
	// ListDataDirectory(recursive bool, includeContents bool) ([]DirEntry, error)
	// log directory operations
}

// ServiceNodeFn performs an operation on a given ServiceNode
type ServiceNodeFn func(ServiceNode) error

// ConcurrentExecutor executes functions on a collection of service
// nodes concurrently
type ConcurrentExecutor interface {
	// Run runs the provide ServiceNodeFn concurrently
	Run() error
}

// HeartbeatRouter routes heartbeats based on registered servers
type HeartbeatRouter interface {
	hb.HeartbeaterServer

	// Endpoint returns the router endpoint
	Endpoint() string

	// Register registers the specified server under the given id
	Register(string, hb.HeartbeaterServer) error

	// Deregister un-registers any server registered under the given id
	Deregister(string) error
}

// ListenerID is a unique identifier for a registered listener
type ListenerID int

// Listener provides callbacks invoked upon remote process state transitions
type Listener interface {
	// OnProcessTerminate is invoked when the remote process being run terminates
	OnProcessTerminate(node ServiceNode, desc string)

	// OnHeartbeatTimeout is invoked upon remote heartbeats having timed-out
	OnHeartbeatTimeout(node ServiceNode, lastHeartbeatTs time.Time)

	// OnOverwrite is invoked if remote agent control is overwritten by another
	// coordinator
	OnOverwrite(node ServiceNode, desc string)
}

// ServiceNodes is a collection of ServiceNode(s)
type ServiceNodes []ServiceNode

// Options are the various knobs to control Node behavior
type Options interface {
	// Validate validates the NodeOptions
	Validate() error

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetOperationTimeout returns the timeout for node operations
	SetOperationTimeout(time.Duration) Options

	// OperationTimeout returns the timeout for node operations
	OperationTimeout() time.Duration

	// SetRetrier sets the retrier for node operations
	SetRetrier(xretry.Retrier) Options

	// OperationRetrier returns the retrier for node operations
	Retrier() xretry.Retrier

	// SetTransferBufferSize sets the bytes buffer size used during file transfer
	SetTransferBufferSize(int) Options

	// TransferBufferSize returns the bytes buffer size used during file transfer
	TransferBufferSize() int

	// SetHeartbeatOptions sets the HeartbeatOptions
	SetHeartbeatOptions(HeartbeatOptions) Options

	// HeartbeatOptions returns the HeartbeatOptions
	HeartbeatOptions() HeartbeatOptions

	// SetOperatorClientFn sets the OperatorClientFn
	SetOperatorClientFn(OperatorClientFn) Options

	// OperatorClientFn returns the OperatorClientFn
	OperatorClientFn() OperatorClientFn
}

// OperatorClientFn returns a function able to construct connections to remote Operators
type OperatorClientFn func() (*grpc.ClientConn, m3em.OperatorClient, error)

// HeartbeatOptions are the knobs to control heartbeating behavior
type HeartbeatOptions interface {
	// Validate validates the HeartbeatOptions
	Validate() error

	// SetEnabled sets whether the Heartbeating is enabled
	SetEnabled(bool) HeartbeatOptions

	// Enabled returns whether the Heartbeating is enabled
	Enabled() bool

	// SetNowFn sets the NowFn
	SetNowFn(xclock.NowFn) HeartbeatOptions

	// NowFn returns the NowFn
	NowFn() xclock.NowFn

	// SetInterval sets the heartbeating interval
	SetInterval(time.Duration) HeartbeatOptions

	// Interval returns the heartbeating interval
	Interval() time.Duration

	// SetCheckInterval sets the frequency with which heartbeating timeouts
	// are checked
	SetCheckInterval(time.Duration) HeartbeatOptions

	// CheckInterval returns the frequency with which heartbeating timeouts
	// are checked
	CheckInterval() time.Duration

	// SetTimeout sets the heartbeat timeout duration, i.e. the window of
	// time after which missing heartbeats are considered errorneous
	SetTimeout(time.Duration) HeartbeatOptions

	// Timeout returns the heartbeat timeout duration, i.e. the window of
	// time after which missing heartbeats are considered errorneous
	Timeout() time.Duration

	// SetHeartbeatRouter sets the heartbeat router to be used
	SetHeartbeatRouter(HeartbeatRouter) HeartbeatOptions

	// HeartbeatRouter returns the heartbeat router in use
	HeartbeatRouter() HeartbeatRouter
}
