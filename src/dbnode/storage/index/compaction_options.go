// Copyright (c) 2018 Uber Technologies, Inc.
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

package index

import (
	"math"
	"runtime"

	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	xsync "github.com/m3db/m3x/sync"
)

type compactionOpts struct {
	plannerOpts compaction.PlannerOptions
	workerPool  xsync.WorkerPool
}

func NewCompactionOptions() CompactionOptions {
	defaultConcurrency := int(math.Max(1, math.Ceil(float64(runtime.NumCPU())/4)))
	workers := xsync.NewWorkerPool(defaultConcurrency)
	workers.Init()
	return &compactionOpts{
		plannerOpts: compaction.DefaultOptions,
		workerPool:  workers,
	}
}

func (co *compactionOpts) SetPlannerOptions(value compaction.PlannerOptions) CompactionOptions {
	opts := *co
	opts.plannerOpts = value
	return &opts
}

func (co *compactionOpts) PlannerOptions() compaction.PlannerOptions {
	return co.plannerOpts
}

func (co *compactionOpts) SetWorkerPool(value xsync.WorkerPool) CompactionOptions {
	opts := *co
	opts.workerPool = value
	return &opts
}

func (co *compactionOpts) WorkerPool() xsync.WorkerPool {
	return co.workerPool
}
