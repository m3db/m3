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
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
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
	Run(
		ctx context.Context,
		start xtime.UnixNano,
		namespaces []ProcessNamespace,
	) (NamespaceResults, error)
}

// ProcessNamespace is a namespace to pass to the bootstrap process.
type ProcessNamespace struct {
	// Metadata of the namespace being bootstrapped.
	Metadata namespace.Metadata
	// Shards is the shards to bootstrap for the bootstrap process.
	Shards []uint32
	// DataAccumulator is the data accumulator for the shards.
	DataAccumulator NamespaceDataAccumulator
	// Hooks is a set of namespace bootstrap hooks.
	Hooks NamespaceHooks
}

// NamespaceHooks is a set of namespace bootstrap hooks.
type NamespaceHooks struct {
	opts NamespaceHooksOptions
}

// Hook wraps a runnable callback.
type Hook interface {
	Run() error
}

// NamespaceHooksOptions is a set of hooks options.
type NamespaceHooksOptions struct {
	BootstrapSourceBegin Hook
	BootstrapSourceEnd   Hook
}

// NewNamespaceHooks returns a new set of bootstrap hooks.
func NewNamespaceHooks(opts NamespaceHooksOptions) NamespaceHooks {
	return NamespaceHooks{opts: opts}
}

// BootstrapSourceBegin is a hook to call when a bootstrap source starts.
func (h NamespaceHooks) BootstrapSourceBegin() error {
	if h.opts.BootstrapSourceBegin == nil {
		return nil
	}
	return h.opts.BootstrapSourceBegin.Run()
}

// BootstrapSourceEnd is a hook to call when a bootstrap source ends.
func (h NamespaceHooks) BootstrapSourceEnd() error {
	if h.opts.BootstrapSourceEnd == nil {
		return nil
	}
	return h.opts.BootstrapSourceEnd.Run()
}

// Namespaces are a set of namespaces being bootstrapped.
type Namespaces struct {
	// Namespaces are the namespaces being bootstrapped.
	Namespaces *NamespacesMap
}

// Hooks returns namespaces hooks for the set of namespaces.
func (n Namespaces) Hooks() NamespacesHooks {
	return NamespacesHooks{namespaces: n.Namespaces}
}

// NamespacesHooks is a helper to run hooks for a set of namespaces.
type NamespacesHooks struct {
	namespaces *NamespacesMap
}

// BootstrapSourceBegin is a hook to call when a bootstrap source starts.
func (h NamespacesHooks) BootstrapSourceBegin() error {
	if h.namespaces == nil {
		return nil
	}

	var (
		wg           sync.WaitGroup
		multiErrLock sync.Mutex
		multiErr     xerrors.MultiError
	)
	for _, elem := range h.namespaces.Iter() {
		ns := elem.Value()

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Run bootstrap source end hook.
			err := ns.Hooks.BootstrapSourceBegin()
			if err == nil {
				return
			}

			multiErrLock.Lock()
			defer multiErrLock.Unlock()

			multiErr = multiErr.Add(err)
		}()
	}

	wg.Wait()

	return multiErr.FinalError()
}

// BootstrapSourceEnd is a hook to call when a bootstrap source starts.
func (h NamespacesHooks) BootstrapSourceEnd() error {
	if h.namespaces == nil {
		return nil
	}

	var (
		wg           sync.WaitGroup
		multiErrLock sync.Mutex
		multiErr     xerrors.MultiError
	)
	for _, elem := range h.namespaces.Iter() {
		ns := elem.Value()

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Run bootstrap source end hook.
			err := ns.Hooks.BootstrapSourceEnd()
			if err == nil {
				return
			}

			multiErrLock.Lock()
			defer multiErrLock.Unlock()

			multiErr = multiErr.Add(err)
		}()
	}

	wg.Wait()

	return multiErr.FinalError()
}

// Namespace is a namespace that is being bootstrapped.
type Namespace struct {
	// Metadata of the namespace being bootstrapped.
	Metadata namespace.Metadata
	// Shards is the shards for the namespace being bootstrapped.
	Shards []uint32
	// DataAccumulator is the data accumulator for the shards.
	DataAccumulator NamespaceDataAccumulator
	// Hooks is a set of namespace bootstrap hooks.
	Hooks NamespaceHooks
	// DataTargetRange is the data target bootstrap range.
	DataTargetRange TargetRange
	// IndexTargetRange is the index target bootstrap range.
	IndexTargetRange TargetRange
	// DataRunOptions are the options for the data bootstrap for this
	// namespace.
	DataRunOptions NamespaceRunOptions
	// IndexRunOptions are the options for the index bootstrap for this
	// namespace.
	IndexRunOptions NamespaceRunOptions
}

// NamespaceRunOptions are the run options for a bootstrap process run.
type NamespaceRunOptions struct {
	// ShardTimeRanges are the time ranges for the shards that should be fulfilled
	// by the bootstrapper. This changes each bootstrapper pass as time ranges are fulfilled.
	ShardTimeRanges result.ShardTimeRanges
	// TargetShardTimeRanges are the original target time ranges for shards and does not change
	// each bootstrapper pass.
	// NB(bodu): This is used by the commit log bootstrapper as it needs to run for the entire original
	// target shard time ranges.
	TargetShardTimeRanges result.ShardTimeRanges
	// RunOptions are the run options for the bootstrap run.
	RunOptions RunOptions
}

// NamespaceDataAccumulator is the namespace data accumulator.
// TODO(r): Consider rename this to a better name.
type NamespaceDataAccumulator interface {
	// CheckoutSeriesWithoutLock retrieves a series for writing to
	// and when the accumulator is closed it will ensure that the
	// series is released.
	//
	// If indexing is not enabled, tags is still required, simply pass
	// ident.EmptyTagIterator.
	//
	// Returns the result, whether the node owns the specified shard, along with
	// an error if any. This allows callers to handle unowned shards differently
	// than other errors. If owned == false, err should not be nil.
	//
	// Note: Without lock variant does not perform any locking and callers
	// must ensure non-parallel access themselves, this helps avoid
	// overhead of the lock for the commit log bootstrapper which reads
	// in a single threaded manner.
	CheckoutSeriesWithoutLock(
		shardID uint32,
		id ident.ID,
		tags ident.TagIterator,
	) (result CheckoutSeriesResult, owned bool, err error)

	// CheckoutSeriesWithLock is the "with lock" version of
	// CheckoutSeriesWithoutLock.
	// Note: With lock variant performs locking and callers do not need
	// to be concerned about parallel access.
	CheckoutSeriesWithLock(
		shardID uint32,
		id ident.ID,
		tags ident.TagIterator,
	) (result CheckoutSeriesResult, owned bool, err error)

	// Close will close the data accumulator and will release
	// all series read/write refs.
	Close() error
}

// CheckoutSeriesResult is the result of a checkout series operation.
type CheckoutSeriesResult struct {
	// Resolver is the series read write ref resolver.
	Resolver SeriesRefResolver
	// Shard is the shard for the series.
	Shard uint32
}

// NamespaceResults is the result of a bootstrap process.
type NamespaceResults struct {
	// Results is the result of a bootstrap process.
	Results *NamespaceResultsMap
}

// NamespaceResult is the result of a bootstrap process for a given namespace.
type NamespaceResult struct {
	Metadata    namespace.Metadata
	Shards      []uint32
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

	// SetTopologyMapProvider sets the TopologyMapProvider.
	SetTopologyMapProvider(value topology.MapProvider) ProcessOptions

	// TopologyMapProvider returns the TopologyMapProvider.
	TopologyMapProvider() topology.MapProvider

	// SetOrigin sets the origin.
	SetOrigin(value topology.Host) ProcessOptions

	// Origin returns the origin.
	Origin() topology.Host

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

// Bootstrapper is the interface for different bootstrapping mechanisms.  Note that a bootstrapper
// can and will be reused so it is important to not rely on state stored in the bootstrapper itself
// with the mindset that it will always be set to default values from the constructor.
type Bootstrapper interface {
	// String returns the name of the bootstrapper
	String() string

	// Bootstrap performs bootstrapping of series data and index metadata
	// for the  given time ranges, returning the bootstrapped series data
	// and index blocks for the time ranges it's unable to fulfill in parallel.
	// A bootstrapper should only return an error should it want to entirely
	// cancel the bootstrapping of the node, i.e. non-recoverable situation
	// like not being able to read from the filesystem.
	Bootstrap(ctx context.Context, namespaces Namespaces, cache Cache) (NamespaceResults, error)
}

// Source represents a bootstrap source. Note that a source can and will be reused so
// it is important to not rely on state stored in the source itself with the mindset
// that it will always be set to default values from the constructor.
type Source interface {
	// AvailableData returns what time ranges are available for bootstrapping a given set of shards.
	AvailableData(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		cache Cache,
		runOpts RunOptions,
	) (result.ShardTimeRanges, error)

	// AvailableIndex returns what time ranges are available for bootstrapping.
	AvailableIndex(
		ns namespace.Metadata,
		shardsTimeRanges result.ShardTimeRanges,
		cache Cache,
		opts RunOptions,
	) (result.ShardTimeRanges, error)

	// Read returns series data and index metadata for a given set of shards
	// and specified time ranges and the time ranges it's unable to fulfill.
	// A bootstrapper source should only return an error should it want to
	// entirely cancel the bootstrapping of the node, i.e. non-recoverable
	// situation like not being able to read from the filesystem.
	Read(ctx context.Context, namespaces Namespaces, cache Cache) (NamespaceResults, error)
}

// InfoFileResultsPerShard maps shards to info files.
type InfoFileResultsPerShard map[uint32][]fs.ReadInfoFileResult

// InfoFilesByNamespace maps a namespace to info files grouped by shard.
type InfoFilesByNamespace map[namespace.Metadata]InfoFileResultsPerShard

// Cache provides a snapshot of info files for use throughout all stages of the bootstrap.
type Cache interface {
	// InfoFilesForNamespace returns the info files grouped by namespace.
	InfoFilesForNamespace(ns namespace.Metadata) (InfoFileResultsPerShard, error)

	// InfoFilesForShard returns the info files grouped by shard for the provided namespace.
	InfoFilesForShard(ns namespace.Metadata, shard uint32) ([]fs.ReadInfoFileResult, error)

	// ReadInfoFiles returns info file results for each shard grouped by namespace. A cached copy
	// is returned if the info files have already been read.
	ReadInfoFiles() InfoFilesByNamespace

	// Evict cache contents by re-reading fresh data in.
	Evict()
}

// CacheOptions represents the options for Cache.
type CacheOptions interface {
	// Validate will validate the options and return an error if not valid.
	Validate() error

	// SetFilesystemOptions sets the filesystem options.
	SetFilesystemOptions(value fs.Options) CacheOptions

	// FilesystemOptions returns the filesystem options.
	FilesystemOptions() fs.Options

	// SetNamespaceDetails sets the namespaces to cache information for.
	SetNamespaceDetails(value []NamespaceDetails) CacheOptions

	// NamespaceDetails returns the namespaces to cache information for.
	NamespaceDetails() []NamespaceDetails

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) CacheOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options
}

// NamespaceDetails are used to lookup info files for the given combination of namespace and shards.
type NamespaceDetails struct {
	// Namespace is the namespace to retrieve info files for.
	Namespace namespace.Metadata
	// Shards are the shards to retrieve info files for in the specified namespace.
	Shards []uint32
}

// SeriesRef is used to both write to and load blocks into a database series.
type SeriesRef interface {
	// Write writes a new value.
	Write(
		ctx context.Context,
		timestamp xtime.UnixNano,
		value float64,
		unit xtime.Unit,
		annotation []byte,
		wOpts series.WriteOptions,
	) (bool, series.WriteType, error)

	// LoadBlock loads a single block into the series.
	LoadBlock(
		block block.DatabaseBlock,
		writeType series.WriteType,
	) error

	// UniqueIndex is the unique index for the series.
	UniqueIndex() uint64
}

// SeriesRefResolver is a series resolver for just in time resolving of
// a series read write ref.
type SeriesRefResolver interface {
	// SeriesRef returns the series read write ref.
	SeriesRef() (SeriesRef, error)
	// ReleaseRef must be called after using the series ref
	// to release the reference count to the series so it can
	// be expired by the owning shard eventually.
	ReleaseRef()
}
