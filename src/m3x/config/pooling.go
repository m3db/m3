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

package config

import "github.com/m3db/m3x/sync"

const (
	defaultWorkerPoolStaticSize = 4096
	defaultGrowKillProbability  = 0.01
)

// WorkerPoolPolicy specifies the policy for the worker pool.
type WorkerPoolPolicy struct {
	// Determines if the worker pool automatically grows to capacity.
	GrowOnDemand bool `yaml:"grow"`

	// Size for static pools, initial size for dynamically growing pools.
	Size int `yaml:"size"`

	// The number of shards for the pool.
	NumShards int64 `yaml:"shards"`

	// The probablility that a worker is killed after completing the task.
	KillWorkerProbability float64 `yaml:"killProbability" validate:"min=0.0,max=1.0"`
}

// Options converts the worker pool policy to options, providing
// the options, as well as the default size for the worker pool.
func (w WorkerPoolPolicy) Options() (sync.PooledWorkerPoolOptions, int) {
	opts := sync.NewPooledWorkerPoolOptions()
	grow := w.GrowOnDemand
	opts = opts.SetGrowOnDemand(grow)
	if w.KillWorkerProbability != 0 {
		opts = opts.SetKillWorkerProbability(w.KillWorkerProbability)
	} else if grow {
		// NB: if using a growing pool, default kill probability is too low, causing
		// the pool to quickly grow out of control. Use a higher default kill probability
		opts = opts.SetKillWorkerProbability(defaultGrowKillProbability)
	}

	if w.NumShards != 0 {
		opts = opts.SetNumShards(w.NumShards)
	}

	if w.Size == 0 {
		if grow {
			w.Size = int(opts.NumShards())
		} else {
			w.Size = defaultWorkerPoolStaticSize
		}
	}

	return opts, w.Size
}
