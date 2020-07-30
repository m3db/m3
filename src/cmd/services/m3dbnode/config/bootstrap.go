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
	"fmt"
	"math"
	"runtime"

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
)

// BootstrapConfiguration specifies the config for bootstrappers.
type BootstrapConfiguration struct {
	// Bootstrappers is the list of bootstrappers, ordered by precedence in
	// descending order.
	Bootstrappers []string `yaml:"bootstrappers" validate:"nonzero"`

	// Filesystem bootstrapper configuration.
	Filesystem *BootstrapFilesystemConfiguration `yaml:"fs"`

	// Commitlog bootstrapper configuration.
	Commitlog *BootstrapCommitlogConfiguration `yaml:"commitlog"`

	// Peers bootstrapper configuration.
	Peers *BootstrapPeersConfiguration `yaml:"peers"`

	// CacheSeriesMetadata determines whether individual bootstrappers cache
	// series metadata across all calls (namespaces / shards / blocks).
	CacheSeriesMetadata *bool `yaml:"cacheSeriesMetadata"`
}

// BootstrapFilesystemConfiguration specifies config for the fs bootstrapper.
type BootstrapFilesystemConfiguration struct {
	// NumProcessorsPerCPU is the number of processors per CPU.
	NumProcessorsPerCPU float64 `yaml:"numProcessorsPerCPU" validate:"min=0.0"`

	// Migrations configuration
	Migrations *BootstrapMigrations `yaml:"migrations"`
}

func (c BootstrapFilesystemConfiguration) numCPUs() int {
	return int(math.Ceil(float64(c.NumProcessorsPerCPU * float64(runtime.NumCPU()))))
}

func (c BootstrapFilesystemConfiguration) migrations() BootstrapMigrations {
	if cfg := c.Migrations; cfg != nil {
		return *cfg
	}
	return BootstrapMigrations{}
}

func newDefaultBootstrapFilesystemConfiguration() BootstrapFilesystemConfiguration {
	return BootstrapFilesystemConfiguration{
		NumProcessorsPerCPU: defaultNumProcessorsPerCPU,
		Migrations:          &BootstrapMigrations{},
	}
}

// BootstrapMigrations specifies configuration for data migrations during bootstrapping
type BootstrapMigrations struct {
	// ToVersion1_1 indicates that we should attempt to upgrade filesets to
	// what’s expected of 1.1 files
	ToVersion1_1 bool `yaml:"toVersion1_1"`
}

// NewOptions generates migration.Options from the configuration
func (m BootstrapMigrations) NewOptions() migration.Options {
	return migration.NewOptions().SetToVersion1_1(m.ToVersion1_1)
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
}

func newDefaultBootstrapPeersConfiguration() BootstrapPeersConfiguration {
	return BootstrapPeersConfiguration{
		StreamShardConcurrency:        peers.DefaultShardConcurrency,
		StreamPersistShardConcurrency: peers.DefaultShardPersistenceConcurrency,
	}
}

// BootstrapConfigurationValidator can be used to validate the option sets
// that the  bootstrap configuration builds.
// Useful for tests and perhaps verifying same options set across multiple
// bootstrappers.
type BootstrapConfigurationValidator interface {
	ValidateBootstrappersOrder(names []string) error
	ValidateFilesystemBootstrapperOptions(opts bfs.Options) error
	ValidateCommitLogBootstrapperOptions(opts commitlog.Options) error
	ValidatePeersBootstrapperOptions(opts peers.Options) error
	ValidateUninitializedBootstrapperOptions(opts uninitialized.Options) error
}

// New creates a bootstrap process based on the bootstrap configuration.
func (bsc BootstrapConfiguration) New(
	validator BootstrapConfigurationValidator,
	rsOpts result.Options,
	opts storage.Options,
	topoMapProvider topology.MapProvider,
	origin topology.Host,
	adminClient client.AdminClient,
) (bootstrap.ProcessProvider, error) {
	if err := validator.ValidateBootstrappersOrder(bsc.Bootstrappers); err != nil {
		return nil, err
	}

	idxOpts := opts.IndexOptions()
	compactor, err := compaction.NewCompactor(idxOpts.DocumentArrayPool(),
		index.DocumentArrayPoolCapacity,
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
		bs     bootstrap.BootstrapperProvider
		fsOpts = opts.CommitLogOptions().FilesystemOptions()
	)
	// Start from the end of the list because the bootstrappers are ordered by precedence in descending order.
	for i := len(bsc.Bootstrappers) - 1; i >= 0; i-- {
		switch bsc.Bootstrappers[i] {
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
				SetCompactor(compactor).
				SetBoostrapDataNumProcessors(fsCfg.numCPUs()).
				SetRuntimeOptionsManager(opts.RuntimeOptionsManager()).
				SetIdentifierPool(opts.IdentifierPool()).
				SetMigrationOptions(fsCfg.migrations().NewOptions())
			if err := validator.ValidateFilesystemBootstrapperOptions(fsbOpts); err != nil {
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
			if err := validator.ValidateCommitLogBootstrapperOptions(cOpts); err != nil {
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
				SetCompactor(compactor).
				SetRuntimeOptionsManager(opts.RuntimeOptionsManager()).
				SetContextPool(opts.ContextPool()).
				SetDefaultShardConcurrency(pCfg.StreamShardConcurrency).
				SetShardPersistenceConcurrency(pCfg.StreamPersistShardConcurrency)
			if err := validator.ValidatePeersBootstrapperOptions(pOpts); err != nil {
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
			if err := validator.ValidateUninitializedBootstrapperOptions(uOpts); err != nil {
				return nil, err
			}
			bs = uninitialized.NewUninitializedTopologyBootstrapperProvider(uOpts, bs)
		default:
			return nil, fmt.Errorf("unknown bootstrapper: %s", bsc.Bootstrappers[i])
		}
	}

	providerOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(topoMapProvider).
		SetOrigin(origin)
	if bsc.CacheSeriesMetadata != nil {
		providerOpts = providerOpts.SetCacheSeriesMetadata(*bsc.CacheSeriesMetadata)
	}
	return bootstrap.NewProcessProvider(bs, providerOpts, rsOpts)
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

type bootstrapConfigurationValidator struct {
}

// NewBootstrapConfigurationValidator returns a new bootstrap configuration
// validator that validates certain options configured by the bootstrap
// configuration.
func NewBootstrapConfigurationValidator() BootstrapConfigurationValidator {
	return bootstrapConfigurationValidator{}
}

func (v bootstrapConfigurationValidator) ValidateBootstrappersOrder(names []string) error {
	dataFetchingBootstrappers := []string{
		bfs.FileSystemBootstrapperName,
		peers.PeersBootstrapperName,
		commitlog.CommitLogBootstrapperName,
	}

	precedingBootstrappersAllowedByBootstrapper := map[string][]string{
		bootstrapper.NoOpAllBootstrapperName:  dataFetchingBootstrappers,
		bootstrapper.NoOpNoneBootstrapperName: dataFetchingBootstrappers,
		bfs.FileSystemBootstrapperName:        []string{
			// Filesystem bootstrapper must always appear first
		},
		peers.PeersBootstrapperName: []string{
			// Peers must always appear after filesystem
			bfs.FileSystemBootstrapperName,
			// Peers may appear before OR after commitlog
			commitlog.CommitLogBootstrapperName,
		},
		commitlog.CommitLogBootstrapperName: []string{
			// Commit log bootstrapper may appear after filesystem or peers
			bfs.FileSystemBootstrapperName,
			peers.PeersBootstrapperName,
		},
		uninitialized.UninitializedTopologyBootstrapperName: []string{
			// Unintialized bootstrapper may appear after filesystem or peers or commitlog
			bfs.FileSystemBootstrapperName,
			commitlog.CommitLogBootstrapperName,
			peers.PeersBootstrapperName,
		},
	}

	validated := make(map[string]struct{})
	for _, name := range names {
		precedingAllowed, ok := precedingBootstrappersAllowedByBootstrapper[name]
		if !ok {
			return fmt.Errorf("unknown bootstrapper: %v", name)
		}

		allowed := make(map[string]struct{})
		for _, elem := range precedingAllowed {
			allowed[elem] = struct{}{}
		}

		for existing := range validated {
			if _, ok := allowed[existing]; !ok {
				return fmt.Errorf("bootstrapper %s cannot appear after %s: ",
					name, existing)
			}
		}

		validated[name] = struct{}{}
	}

	return nil
}

func (v bootstrapConfigurationValidator) ValidateFilesystemBootstrapperOptions(
	opts bfs.Options,
) error {
	return opts.Validate()
}

func (v bootstrapConfigurationValidator) ValidateCommitLogBootstrapperOptions(
	opts commitlog.Options,
) error {
	return opts.Validate()
}

func (v bootstrapConfigurationValidator) ValidatePeersBootstrapperOptions(
	opts peers.Options,
) error {
	return opts.Validate()
}

func (v bootstrapConfigurationValidator) ValidateUninitializedBootstrapperOptions(
	opts uninitialized.Options,
) error {
	return opts.Validate()
}
