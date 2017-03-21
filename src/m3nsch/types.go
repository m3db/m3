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

package m3nsch

import (
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"
)

// Status represents the various states the load generation processes may exist in.
type Status int

const (
	// StatusUninitialized refers to a load generation process yet to be initialized.
	StatusUninitialized Status = iota

	// StatusInitialized refers to a load generation process which has been initialized.
	StatusInitialized

	// StatusRunning refers to a load generation process which is running.
	StatusRunning
)

// Workload is a collection of attributes required to define a load generation workload.
// TODO(prateek): add support for duration
type Workload struct {
	// BaseTime is the epoch value for time used during load generation, all timestamps are
	// generated relative to it.
	BaseTime time.Time

	// Namespace is the target namespace to perform the writes against.
	Namespace string

	// MetricPrefix is the string prefixed to each metric used.
	MetricPrefix string

	// Cardinality is the number of unique metrics used.
	Cardinality int

	// IngressQPS is the number of metrics written per second.
	IngressQPS int

	// MetricStartIdx is an offset to control metric numbering. Can be safely ignored
	// by external callers.
	MetricStartIdx int
}

// Coordinator refers to the process responsible for synchronizing load generation.
type Coordinator interface {
	// Status returns the status of the agents known to this process.
	Status() (map[string]AgentStatus, error)

	// Init acquires resources required on known agent processes to be able to
	// generate load.
	// NB(prateek): Init takes ~30s to initialize m3db.Session objects
	Init(token string, w Workload, force bool, targetZone string, targetEnv string) error

	// Workload returns the aggregate workload currently initialized on the
	// agent processes known to the coordinator process.
	Workload() (Workload, error)

	// SetWorkload splits the specified workload into smaller chunks, and distributes them
	// across known agent processes.
	SetWorkload(Workload) error

	// Start begins the load generation process on known agent processes.
	Start() error

	// Stops ends the load generation process on known agent processes.
	Stop() error

	// Teardown releases resources held by the Coordinator process (connections, state tracking structures).
	Teardown() error
}

// CoordinatorOptions is a collection of the various knobs to control Coordinator behavior.
type CoordinatorOptions interface {
	// SetInstrumentOptions sets the InstrumentOptions
	SetInstrumentOptions(instrument.Options) CoordinatorOptions

	// InstrumentOptions returns the set InstrumentOptions
	InstrumentOptions() instrument.Options

	// SetTimeout sets the timeout for rpc interaction
	SetTimeout(time.Duration) CoordinatorOptions

	// Timeout returns the timeout for rpc interaction
	Timeout() time.Duration

	// SetParallelOperations sets a flag determining if the operations
	// performed by the coordinator against various endpoints are to be
	// parallelized or not
	SetParallelOperations(bool) CoordinatorOptions

	// ParallelOperations returns a flag indicating if the operations
	// performed by the coordinator against various endpoints are to be
	// parallelized or not
	ParallelOperations() bool
}

// AgentStatus is a collection of attributes capturing the state
// of a agent process.
type AgentStatus struct {
	// Status refers to the agent process' running status
	Status Status

	// Token (if non-empty) is the breadcrumb used to Init the agent process'
	Token string

	// MaxQPS is the maximum QPS attainable by the Agent process
	MaxQPS int64

	// Workload is the currently configured workload on the agent process
	Workload Workload
}

// Agent refers to the process responsible for executing load generation.
type Agent interface {
	// Status returns the status of the agent process.
	Status() AgentStatus

	// Workload returns Workload currently configured on the agent process.
	Workload() Workload

	// SetWorkload sets the Workload on the agent process.
	SetWorkload(Workload)

	// Init initializes resources required by the agent process.
	Init(token string, w Workload, force bool, targetZone string, targetEnv string) error

	// Start begins the load generation process if the agent is Initialized, or errors if it is not.
	Start() error

	// Stop ends the load generation process if the agent is Running.
	Stop() error

	// TODO(prateek): stats

	// MaxQPS returns the maximum QPS this Agent is capable of driving.
	// MaxQPS := `AgentOptions.MaxWorkerQPS() * AgentOptions.Concurrency()`
	MaxQPS() int64
}

// NewSessionFn creates a new client.Session for the specified environment, zone.
type NewSessionFn func(targetZone string, targetEnv string) (client.Session, error)

// AgentOptions is a collection of knobs to control Agent behavior.
type AgentOptions interface {
	// SetInstrumentOptions sets the InstrumentOptions
	SetInstrumentOptions(instrument.Options) AgentOptions

	// InstrumentOptions returns the set InstrumentOptions
	InstrumentOptions() instrument.Options

	// SetMaxWorkerQPS sets the maximum QPS per 'worker' go-routine used in
	// the load generation process.
	SetMaxWorkerQPS(int64) AgentOptions

	// SetMaxWorkerQPS returns the maximum QPS per 'worker' go-routine used in
	// the load generation process.
	MaxWorkerQPS() int64

	// SetConcurrency sets the number of concurrent go routines used.
	SetConcurrency(int) AgentOptions

	// Concurrency returns the number of concurrent go routines usable during
	// load generation.
	Concurrency() int

	// SetNewSessionFn sets the new session function
	SetNewSessionFn(fn NewSessionFn) AgentOptions

	// NewSessionFn returns the new session function
	NewSessionFn() NewSessionFn

	// SetTimeUnit sets the time unit to use during load operations.
	SetTimeUnit(xtime.Unit) AgentOptions

	// TimeUnit returns the time unit used during load operations.
	TimeUnit() xtime.Unit
}
