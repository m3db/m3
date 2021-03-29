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

package config

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	bfs "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/uninitialized"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
)

var (
	// defaultNumProcessorsPerCPU is the default number of processors per CPU.
	defaultNumProcessorsPerCPU = 0.125

	// default order in which bootstrappers are run
	// (run in ascending order of precedence).
	defaultOrderedBootstrappers = []string{
		// Filesystem bootstrapping must be first.
		bfs.FileSystemBootstrapperName,
		// Peers and commitlog must come before the uninitialized topology bootrapping.
		commitlog.CommitLogBootstrapperName,
		peers.PeersBootstrapperName,
		uninitialized.UninitializedTopologyBootstrapperName,
	}

	// bootstrapper order where peers is prefered over commitlog.
	preferPeersOrderedBootstrappers = []string{
		// Filesystem bootstrapping must be first.
		bfs.FileSystemBootstrapperName,
		// Prefer peers over commitlog.
		peers.PeersBootstrapperName,
		commitlog.CommitLogBootstrapperName,
		uninitialized.UninitializedTopologyBootstrapperName,
	}

	// bootstrapper order where commitlog is excluded.
	excludeCommitLogOrderedBootstrappers = []string{
		// Filesystem bootstrapping must be first.
		bfs.FileSystemBootstrapperName,
		// Commitlog excluded.
		peers.PeersBootstrapperName,
		uninitialized.UninitializedTopologyBootstrapperName,
	}

	validBootstrapModes = []BootstrapMode{
		DefaultBootstrapMode,
		PreferPeersBootstrapMode,
		ExcludeCommitLogBootstrapMode,
	}

	errReadBootstrapModeInvalid = errors.New("bootstrap mode invalid")
)

// BootstrapMode defines the mode in which bootstrappers are run.
type BootstrapMode uint

const (
	// DefaultBootstrapMode executes bootstrappers in default order.
	DefaultBootstrapMode BootstrapMode = iota
	// PreferPeersBootstrapMode executes peers before commitlog bootstrapper.
	PreferPeersBootstrapMode
	// ExcludeCommitLogBootstrapMode executes all default bootstrappers except commitlog.
	ExcludeCommitLogBootstrapMode
)

// UnmarshalYAML unmarshals an BootstrapMode into a valid type from string.
func (m *BootstrapMode) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	// If unspecified, use default mode.
	if str == "" {
		*m = DefaultBootstrapMode
		return nil
	}

	for _, valid := range validBootstrapModes {
		if str == valid.String() {
			*m = valid
			return nil
		}
	}
	return fmt.Errorf("invalid BootstrapMode '%s' valid types are: %s",
		str, validBootstrapModes)
}

// String returns the bootstrap mode as a string
func (m BootstrapMode) String() string {
	switch m {
	case DefaultBootstrapMode:
		return "default"
	case PreferPeersBootstrapMode:
		return "prefer_peers"
	case ExcludeCommitLogBootstrapMode:
		return "exclude_commitlog"
	}
	return "unknown"
}

// BootstrapConfiguration specifies the config for bootstrappers.
type BootstrapConfiguration struct {
	// BootstrapMode defines the mode in which bootstrappers are run.
	BootstrapMode *BootstrapMode `yaml:"mode"`

	// Filesystem bootstrapper configuration.
	Filesystem *BootstrapFilesystemConfiguration `yaml:"filesystem"`

	// Commitlog bootstrapper configuration.
	Commitlog *BootstrapCommitlogConfiguration `yaml:"commitlog"`

	// Peers bootstrapper configuration.
	Peers *BootstrapPeersConfiguration `yaml:"peers"`

	// CacheSeriesMetadata determines whether individual bootstrappers cache
	// series metadata across all calls (namespaces / shards / blocks).
	CacheSeriesMetadata *bool `yaml:"cacheSeriesMetadata"`

	// IndexSegmentConcurrency determines the concurrency for building index
	// segments.
	IndexSegmentConcurrency *int `yaml:"indexSegmentConcurrency"`

	// Verify specifies verification checks.
	Verify *BootstrapVerifyConfiguration `yaml:"verify"`
}

// VerifyOrDefault returns verify configuration or default.
func (bsc BootstrapConfiguration) VerifyOrDefault() BootstrapVerifyConfiguration {
	if bsc.Verify == nil {
		return BootstrapVerifyConfiguration{}
	}

	return *bsc.Verify
}

// BootstrapVerifyConfiguration outlines verification checks to enable
// during a bootstrap.
type BootstrapVerifyConfiguration struct {
	VerifyIndexSegments *bool `yaml:"verifyIndexSegments"`
}

// VerifyIndexSegmentsOrDefault returns whether to verify index segments
// or use default value.
func (c BootstrapVerifyConfiguration) VerifyIndexSegmentsOrDefault() bool {
	if c.VerifyIndexSegments == nil {
		return false
	}

	return *c.VerifyIndexSegments
}

// BootstrapFilesystemConfiguration specifies config for the fs bootstrapper.
type BootstrapFilesystemConfiguration struct {
	// DeprecatedNumProcessorsPerCPU is the number of processors per CPU.
	// TODO: Remove, this is deprecated since BootstrapDataNumProcessors() is
	// no longer actually used anywhere.
	DeprecatedNumProcessorsPerCPU float64 `yaml:"numProcessorsPerCPU" validate:"min=0.0"`

	// Migration configuration specifies what version, if any, existing data filesets should be migrated to
	// if necessary.
	Migration *BootstrapMigrationConfiguration `yaml:"migration"`
}

func (c BootstrapFilesystemConfiguration) migration() BootstrapMigrationConfiguration {
	if cfg := c.Migration; cfg != nil {
		return *cfg
	}
	return BootstrapMigrationConfiguration{}
}

func newDefaultBootstrapFilesystemConfiguration() BootstrapFilesystemConfiguration {
	return BootstrapFilesystemConfiguration{
		Migration: &BootstrapMigrationConfiguration{},
	}
}

// BootstrapMigrationConfiguration specifies configuration for data migrations during bootstrapping.
type BootstrapMigrationConfiguration struct {
	// TargetMigrationVersion indicates that we should attempt to upgrade filesets to
	// what’s expected of the specified version.
	TargetMigrationVersion migration.MigrationVersion `yaml:"targetMigrationVersion"`

	// Concurrency sets the number of concurrent workers performing migrations.
	Concurrency int `yaml:"concurrency"`
}

// NewOptions generates migration.Options from the configuration.
func (m BootstrapMigrationConfiguration) NewOptions() migration.Options {
	opts := migration.NewOptions().SetTargetMigrationVersion(m.TargetMigrationVersion)

	if m.Concurrency > 0 {
		opts = opts.SetConcurrency(m.Concurrency)
	}

	return opts
}

// BootstrapCommitlogConfiguration specifies config for the commitlog bootstrapper.
type BootstrapCommitlogConfiguration struct {
	// ReturnUnfulfilledForCorruptCommitLogFiles controls whether the commitlog bootstrapper
	// will return unfulfilled for all shard time ranges when it encounters a corrupt commit
	// file. Note that regardless of this value, the commitlog bootstrapper will still try and
	// read all the uncorrupted commitlog files and return as much data as it can, but setting
	// this to true allows the node to attempt a repair if the peers bootstrapper is configured
	// after the commitlog bootstrapper.
	ReturnUnfulfilledForCorruptCommitLogFiles bool `yaml:"returnUnfulfilledForCorruptCommitLogFiles"`
}

func newDefaultBootstrapCommitlogConfiguration() BootstrapCommitlogConfiguration {
	return BootstrapCommitlogConfiguration{
		ReturnUnfulfilledForCorruptCommitLogFiles: commitlog.DefaultReturnUnfulfilledForCorruptCommitLogFiles,
	}
}

// BootstrapPeersConfiguration specifies config for the peers bootstrapper.
type BootstrapPeersConfiguration struct {
	// StreamShardConcurrency controls how many shards in parallel to stream
	// for in memory data being streamed between peers (most recent block).
	// Defaults to: numCPU.
	StreamShardConcurrency int `yaml:"streamShardConcurrency"`
	// StreamPersistShardConcurrency controls how many shards in parallel to stream
	// for historical data being streamed between peers (historical blocks).
	// Defaults to: numCPU / 2.
	StreamPersistShardConcurrency int `yaml:"streamPersistShardConcurrency"`
	// StreamPersistShardFlushConcurrency controls how many shards in parallel to flush
	// for historical data being streamed between peers (historical blocks).
	// Defaults to: 1.
	StreamPersistShardFlushConcurrency int `yaml:"streamPersistShardFlushConcurrency"`
}

func newDefaultBootstrapPeersConfiguration() BootstrapPeersConfiguration {
	return BootstrapPeersConfiguration{
		StreamShardConcurrency:             peers.DefaultShardConcurrency,
		StreamPersistShardConcurrency:      peers.DefaultShardPersistenceConcurrency,
		StreamPersistShardFlushConcurrency: peers.DefaultShardPersistenceFlushConcurrency,
	}
}

// New creates a bootstrap process based on the bootstrap configuration.
func (bsc BootstrapConfiguration) New(
	rsOpts result.Options,
	opts storage.Options,
	topoMapProvider topology.MapProvider,
	origin topology.Host,
	adminClient client.AdminClient,
) (bootstrap.ProcessProvider, error) {
	idxOpts := opts.IndexOptions()
	compactor, err := compaction.NewCompactor(idxOpts.MetadataArrayPool(),
		index.MetadataArrayPoolCapacity,
		idxOpts.SegmentBuilderOptions(),
		idxOpts.FSTSegmentOptions(),
		compaction.CompactorOptions{
			FSTWriterOptions: &fst.WriterOptions{
				// DisableRegistry is set to true to trade a larger FST size
				// for a faster FST compaction since we want to reduce the end
				// to end latency for time to first index a metric.
				DisableRegistry: true,
			},
		})
	if err != nil {
		return nil, err
	}

	var (
		bs                   bootstrap.BootstrapperProvider
		fsOpts               = opts.CommitLogOptions().FilesystemOptions()
		orderedBootstrappers = bsc.orderedBootstrappers()
	)
	// Start from the end of the list because the bootstrappers are ordered by precedence in descending order.
	// I.e. each bootstrapper wraps the preceding bootstrapper, and so the outer-most bootstrapper is run first.
	for i := len(orderedBootstrappers) - 1; i >= 0; i-- {
		switch orderedBootstrappers[i] {
		case bootstrapper.NoOpAllBootstrapperName:
			bs = bootstrapper.NewNoOpAllBootstrapperProvider()
		case bootstrapper.NoOpNoneBootstrapperName:
			bs = bootstrapper.NewNoOpNoneBootstrapperProvider()
		case bfs.FileSystemBootstrapperName:
			fsCfg := bsc.filesystemConfig()
			fsbOpts := bfs.NewOptions().
				SetInstrumentOptions(opts.InstrumentOptions()).
				SetResultOptions(rsOpts).
				SetFilesystemOptions(fsOpts).
				SetIndexOptions(opts.IndexOptions()).
				SetPersistManager(opts.PersistManager()).
				SetIndexClaimsManager(opts.IndexClaimsManager()).
				SetCompactor(compactor).
				SetRuntimeOptionsManager(opts.RuntimeOptionsManager()).
				SetIdentifierPool(opts.IdentifierPool()).
				SetMigrationOptions(fsCfg.migration().NewOptions()).
				SetStorageOptions(opts).
				SetIndexSegmentsVerify(bsc.VerifyOrDefault().VerifyIndexSegmentsOrDefault())
			if v := bsc.IndexSegmentConcurrency; v != nil {
				fsbOpts = fsbOpts.SetIndexSegmentConcurrency(*v)
			}
			if err := fsbOpts.Validate(); err != nil {
				return nil, err
			}
			bs, err = bfs.NewFileSystemBootstrapperProvider(fsbOpts, bs)
			if err != nil {
				return nil, err
			}
		case commitlog.CommitLogBootstrapperName:
			cCfg := bsc.commitlogConfig()
			cOpts := commitlog.NewOptions().
				SetResultOptions(rsOpts).
				SetCommitLogOptions(opts.CommitLogOptions()).
				SetRuntimeOptionsManager(opts.RuntimeOptionsManager()).
				SetReturnUnfulfilledForCorruptCommitLogFiles(cCfg.ReturnUnfulfilledForCorruptCommitLogFiles)
			if err := cOpts.Validate(); err != nil {
				return nil, err
			}
			inspection, err := fs.InspectFilesystem(fsOpts)
			if err != nil {
				return nil, err
			}
			bs, err = commitlog.NewCommitLogBootstrapperProvider(cOpts, inspection, bs)
			if err != nil {
				return nil, err
			}
		case peers.PeersBootstrapperName:
			pCfg := bsc.peersConfig()
			pOpts := peers.NewOptions().
				SetResultOptions(rsOpts).
				SetFilesystemOptions(fsOpts).
				SetIndexOptions(opts.IndexOptions()).
				SetAdminClient(adminClient).
				SetPersistManager(opts.PersistManager()).
				SetIndexClaimsManager(opts.IndexClaimsManager()).
				SetCompactor(compactor).
				SetRuntimeOptionsManager(opts.RuntimeOptionsManager()).
				SetContextPool(opts.ContextPool()).
				SetDefaultShardConcurrency(pCfg.StreamShardConcurrency).
				SetShardPersistenceConcurrency(pCfg.StreamPersistShardConcurrency).
				SetShardPersistenceFlushConcurrency(pCfg.StreamPersistShardFlushConcurrency)
			if v := bsc.IndexSegmentConcurrency; v != nil {
				pOpts = pOpts.SetIndexSegmentConcurrency(*v)
			}
			if err := pOpts.Validate(); err != nil {
				return nil, err
			}
			bs, err = peers.NewPeersBootstrapperProvider(pOpts, bs)
			if err != nil {
				return nil, err
			}
		case uninitialized.UninitializedTopologyBootstrapperName:
			uOpts := uninitialized.NewOptions().
				SetResultOptions(rsOpts).
				SetInstrumentOptions(opts.InstrumentOptions())
			if err := uOpts.Validate(); err != nil {
				return nil, err
			}
			bs = uninitialized.NewUninitializedTopologyBootstrapperProvider(uOpts, bs)
		default:
			return nil, fmt.Errorf("unknown bootstrapper: %s", orderedBootstrappers[i])
		}
	}

	providerOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(topoMapProvider).
		SetOrigin(origin)
	if bsc.CacheSeriesMetadata != nil {
		providerOpts = providerOpts.SetCacheSeriesMetadata(*bsc.CacheSeriesMetadata)
	}
	return bootstrap.NewProcessProvider(bs, providerOpts, rsOpts, fsOpts)
}

func (bsc BootstrapConfiguration) filesystemConfig() BootstrapFilesystemConfiguration {
	if cfg := bsc.Filesystem; cfg != nil {
		return *cfg
	}
	return newDefaultBootstrapFilesystemConfiguration()
}

func (bsc BootstrapConfiguration) commitlogConfig() BootstrapCommitlogConfiguration {
	if cfg := bsc.Commitlog; cfg != nil {
		return *cfg
	}
	return newDefaultBootstrapCommitlogConfiguration()
}

func (bsc BootstrapConfiguration) peersConfig() BootstrapPeersConfiguration {
	if cfg := bsc.Peers; cfg != nil {
		return *cfg
	}
	return newDefaultBootstrapPeersConfiguration()
}

func (bsc BootstrapConfiguration) orderedBootstrappers() []string {
	if bsc.BootstrapMode != nil {
		switch *bsc.BootstrapMode {
		case DefaultBootstrapMode:
			return defaultOrderedBootstrappers
		case PreferPeersBootstrapMode:
			return preferPeersOrderedBootstrappers
		case ExcludeCommitLogBootstrapMode:
			return excludeCommitLogOrderedBootstrappers
		}
	}
	return defaultOrderedBootstrappers
}
