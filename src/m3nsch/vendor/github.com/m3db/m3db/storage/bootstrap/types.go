// Copyright (c) 2016 Uber Technologies, Inc.
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

package bootstrap

import (
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	xtime "github.com/m3db/m3x/time"
)

// Process represents the bootstrap process. Note that a bootstrap process can and will
// be reused so it is important to not rely on state stored in the bootstrap itself
// with the mindset that it will always be set to default values from the constructor.
type Process interface {
	// SetBootstrapper sets the bootstrapper to use when running the process.
	SetBootstrapper(bootstrapper Bootstrapper)

	// Bootstrapper returns the current bootstrapper to use when running the process.
	Bootstrapper() Bootstrapper

	// Run runs the bootstrap process, returning the bootstrap result and any error encountered.
	Run(
		namespace ts.ID,
		shards []uint32,
		targetRanges []TargetRange,
	) (result.BootstrapResult, error)
}

// TargetRange is a bootstrap target range.
type TargetRange struct {
	// Range is the time range to bootstrap for.
	Range xtime.Range

	// RunOptions is the bootstrap run options specific to the target range.
	RunOptions RunOptions
}

// RunOptions is a set of options for a bootstrap run.
type RunOptions interface {
	// SetIncremental sets whether this bootstrap should be an incremental
	// that saves intermediate results to durable storage or not.
	SetIncremental(value bool) RunOptions

	// Incremental returns whether this bootstrap should be an incremental
	// that saves intermediate results to durable storage or not.
	Incremental() bool
}

// Strategy describes a bootstrap strategy.
type Strategy int

const (
	// BootstrapSequential describes whether a bootstrap can use the sequential bootstrap strategy.
	BootstrapSequential Strategy = iota
	// BootstrapParallel describes whether a bootstrap can use the parallel bootstrap strategy.
	BootstrapParallel
)

// Bootstrapper is the interface for different bootstrapping mechanisms.  Note that a bootstrapper
// can and will be reused so it is important to not rely on state stored in the bootstrapper itself
// with the mindset that it will always be set to default values from the constructor.
type Bootstrapper interface {
	// String returns the name of the bootstrapper
	String() string

	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	// Bootstrap performs bootstrapping for the given time ranges, returning the bootstrapped
	// series data and the time ranges it's unable to fulfill in parallel. A bootstrapper
	// should only return an error should it want to entirely cancel the bootstrapping of the
	// node, i.e. non-recoverable situation like not being able to read from the filesystem.
	Bootstrap(
		namespace ts.ID,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.BootstrapResult, error)
}

// Source represents a bootstrap source. Note that a source can and will be reused so
// it is important to not rely on state stored in the source itself with the mindset
// that it will always be set to default values from the constructor.
type Source interface {
	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	// Available returns what time ranges are available for a given set of shards.
	Available(
		namespace ts.ID,
		shardsTimeRanges result.ShardTimeRanges,
	) result.ShardTimeRanges

	// Read returns raw series for a given set of shards & specified time ranges and
	// the time ranges it's unable to fulfill. A bootstrapper source should only return
	// an error should it want to entirely cancel the bootstrapping of the node,
	// i.e. non-recoverable situation like not being able to read from the filesystem.
	Read(
		namespace ts.ID,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.BootstrapResult, error)
}
