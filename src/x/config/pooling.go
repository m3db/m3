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

import "github.com/m3db/m3/src/x/sync"

// WorkerPoolPolicy specifies the policy for the worker pool.
type WorkerPoolPolicy struct {
	// Determines if the worker pool automatically grows to capacity.
	// Deprecated: not used. Pools are explicitly defined as static or dynamic.
	GrowOnDemand bool `yaml:"grow"`

	// Size for static pools, initial size for dynamically growing pools.
	// Deprecated: not used. NumShards controls the size of the pool.
	Size int `yaml:"size"`

	// NumShards is the number of worker channels in the pool.
	NumShards int `yaml:"shards"`

	// The probability that a worker is killed after completing the task.
	KillWorkerProbability float64 `yaml:"killProbability" validate:"min=0.0,max=1.0"`
}

// Options converts policy to options
func (w WorkerPoolPolicy) Options() sync.PooledWorkerPoolOptions {
	opts := sync.NewPooledWorkerPoolOptions()
	if w.KillWorkerProbability != 0 {
		opts = opts.SetKillWorkerProbability(w.KillWorkerProbability)
	}
	if w.NumShards != 0 {
		opts = opts.SetNumShards(w.NumShards)
	}
	return opts
}
