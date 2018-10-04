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
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	xtime "github.com/m3db/m3x/time"
)

// ProcessProvider constructs a bootstrap process that can execute a
// bootstrap run.
type ProcessProvider interface {
	// SetBootstrapper sets the bootstrapper provider to use when running the
	// process.
	SetBootstrapperProvider(bootstrapper BootstrapperProvider)

	// Bootstrapper returns the current bootstrappe provider to use when
	// running the process.
	BootstrapperProvider() BootstrapperProvider

	// Provide constructs a bootstrap process.
	Provide() (Process, error)
}

// Process represents the bootstrap process. Note that a bootstrap process can and will
// be reused so it is important to not rely on state stored in the bootstrap itself
// with the mindset that it will always be set to default values from the constructor.
type Process interface {
	// Run runs the bootstrap process, returning the bootstrap result and any error encountered.
	Run(start time.Time, ns namespace.Metadata, shards []uint32) (ProcessResult, error)
}

// ProcessResult is the result of a bootstrap process.
type ProcessResult struct {
	DataResult  result.DataBootstrapResult
	IndexResult result.IndexBootstrapResult
}

// TargetRange is a bootstrap target range.
type TargetRange struct {
	// Range is the time range to bootstrap for.
	Range xtime.Range

	// RunOptions is the bootstrap run options specific to the target range.
	RunOptions RunOptions
}

// PersistConfig is the configuration for a bootstrap with persistence.
type PersistConfig struct {
	// If enabled bootstrappers are allowed to write out bootstrapped data
	// to disk on their own instead of just returning result in-memory.
	Enabled bool
	// If enabled, what type of persistence files should be generated during
	// the process.
	FileSetType persist.FileSetType
}

// ProcessOptions is a set of options for a bootstrap provider.
type ProcessOptions interface {
	// SetCacheSeriesMetadata sets whether bootstrappers created by this
	// provider should cache series metadata between runs.
	SetCacheSeriesMetadata(value bool) ProcessOptions

	// CacheSeriesMetadata returns whether bootstrappers created by this
	// provider should cache series metadata between runs.
	CacheSeriesMetadata() bool

	// SetAdminClient sets the admin client.
	SetAdminClient(value client.AdminClient) ProcessOptions

	// AdminClient returns the admin client.
	AdminClient() client.AdminClient

	// Validate validates that the ProcessOptions are correct.
	Validate() error
}

// RunOptions is a set of options for a bootstrap run.
type RunOptions interface {
	// SetPersistConfig sets persistence configuration for this bootstrap.
	SetPersistConfig(value PersistConfig) RunOptions

	// PersistConfig returns the persistence configuration for this bootstrap.
	PersistConfig() PersistConfig

	// SetCacheSeriesMetadata sets whether bootstrappers created by this
	// provider should cache series metadata between runs.
	SetCacheSeriesMetadata(value bool) RunOptions

	// CacheSeriesMetadata returns whether bootstrappers created by this
	// provider should cache series metadata between runs.
	CacheSeriesMetadata() bool

	// SetInitialTopologyState sets the initial topology state as it was
	// measured before the bootstrap process began.
	SetInitialTopologyState(value *topology.StateSnapshot) RunOptions

	// InitialTopologyState returns the initial topology as it was measured
	// before the bootstrap process began.
	InitialTopologyState() *topology.StateSnapshot
}

// BootstrapperProvider constructs a bootstrapper.
type BootstrapperProvider interface {
	// String returns the name of the bootstrapper.
	String() string

	// Provide constructs a bootstrapper.
	Provide() (Bootstrapper, error)
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

	// BootstrapData performs bootstrapping of data for the given time ranges, returning the bootstrapped
	// series data and the time ranges it's unable to fulfill in parallel. A bootstrapper
	// should only return an error should it want to entirely cancel the bootstrapping of the
	// node, i.e. non-recoverable situation like not being able to read from the filesystem.
	BootstrapData(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.DataBootstrapResult, error)

	// BootstrapIndex performs bootstrapping of index blocks for the given time ranges, returning
	// the bootstrapped index blocks and the time ranges it's unable to fulfill in parallel. A bootstrapper
	// should only return an error should it want to entirely cancel the bootstrapping of the
	// node, i.e. non-recoverable situation like not being able to read from the filesystem.
	BootstrapIndex(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.IndexBootstrapResult, error)
}

// Source represents a bootstrap source. Note that a source can and will be reused so
// it is important to not rely on state stored in the source itself with the mindset
// that it will always be set to default values from the constructor.
type Source interface {
	// Can returns whether a specific bootstrapper strategy can be applied.
	Can(strategy Strategy) bool

	// AvailableData returns what time ranges are available for bootstrapping a given set of shards.
	AvailableData(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		runOpts RunOptions,
	) (result.ShardTimeRanges, error)

	// ReadData returns raw series for a given set of shards & specified time ranges and
	// the time ranges it's unable to fulfill. A bootstrapper source should only return
	// an error should it want to entirely cancel the bootstrapping of the node,
	// i.e. non-recoverable situation like not being able to read from the filesystem.
	ReadData(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		runOpts RunOptions,
	) (result.DataBootstrapResult, error)

	// AvailableIndex returns what time ranges are available for bootstrapping.
	AvailableIndex(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.ShardTimeRanges, error)

	// ReadIndex returns series index blocks.
	ReadIndex(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		opts RunOptions,
	) (result.IndexBootstrapResult, error)
}
