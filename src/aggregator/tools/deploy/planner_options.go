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

package deploy

import (
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3x/sync"
)

const (
	defaultPlannerWorkerPoolSize = 16
	defaultElectionKeyFmt        = "/shardset/%s/lock"
	defaultMaxStepSize           = 0
)

// PlannerOptions provide a set of options for the deployment planner.
type PlannerOptions interface {
	// SetLeaderService sets the leader service.
	SetLeaderService(value services.LeaderService) PlannerOptions

	// LeaderService returns the leader service.
	LeaderService() services.LeaderService

	// SetWorkerPool sets the worker pool.
	SetWorkerPool(value xsync.WorkerPool) PlannerOptions

	// WorkerPool returns the worker pool.
	WorkerPool() xsync.WorkerPool

	// SetElectionKeyFmt sets the election key format.
	SetElectionKeyFmt(value string) PlannerOptions

	// ElectionKeyFmt returns the election key format.
	ElectionKeyFmt() string

	// SetMaxStepSize sets the maximum step size (i.e., number of instances per step).
	SetMaxStepSize(value int) PlannerOptions

	// MaxStepSize returns the maximum step size (i.e., number of instances per step).
	MaxStepSize() int
}

type plannerOptions struct {
	leaderService  services.LeaderService
	workerPool     xsync.WorkerPool
	electionKeyFmt string
	maxStepSize    int
}

// NewPlannerOptions create a new set of options for the deployment planner.
func NewPlannerOptions() PlannerOptions {
	workers := xsync.NewWorkerPool(defaultPlannerWorkerPoolSize)
	workers.Init()
	return &plannerOptions{
		workerPool:     workers,
		electionKeyFmt: defaultElectionKeyFmt,
		maxStepSize:    defaultMaxStepSize,
	}
}

func (o *plannerOptions) SetLeaderService(value services.LeaderService) PlannerOptions {
	opts := *o
	opts.leaderService = value
	return &opts
}

func (o *plannerOptions) LeaderService() services.LeaderService {
	return o.leaderService
}

func (o *plannerOptions) SetWorkerPool(value xsync.WorkerPool) PlannerOptions {
	opts := *o
	opts.workerPool = value
	return &opts
}

func (o *plannerOptions) WorkerPool() xsync.WorkerPool {
	return o.workerPool
}

func (o *plannerOptions) SetElectionKeyFmt(value string) PlannerOptions {
	opts := *o
	opts.electionKeyFmt = value
	return &opts
}

func (o *plannerOptions) ElectionKeyFmt() string {
	return o.electionKeyFmt
}

func (o *plannerOptions) SetMaxStepSize(value int) PlannerOptions {
	opts := *o
	opts.maxStepSize = value
	return &opts
}

func (o *plannerOptions) MaxStepSize() int {
	return o.maxStepSize
}
