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
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	bfs "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/uninitialized"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/topology"
)

var (
	// defaultNumProcessorsPerCPU is the default number of processors per CPU.
	defaultNumProcessorsPerCPU = 0.5
)

// BootstrapConfiguration specifies the config for bootstrappers.
type BootstrapConfiguration struct {
	// Bootstrappers is the list of bootstrappers, ordered by precedence in
	// descending order.
	Bootstrappers []string `yaml:"bootstrappers" validate:"nonzero"`

	// Filesystem bootstrapper configuration.
	Filesystem *BootstrapFilesystemConfiguration `yaml:"fs"`

	// Peers bootstrapper configuration.
	Peers *BootstrapPeersConfiguration `yaml:"peers"`

	// CacheSeriesMetadata determines whether individual bootstrappers cache
	// series metadata across all calls (namespaces / shards / blocks).
	CacheSeriesMetadata *bool `yaml:"cacheSeriesMetadata"`
}

func (bsc BootstrapConfiguration) fsNumProcessors() int {
	np := defaultNumProcessorsPerCPU
	if fsCfg := bsc.Filesystem; fsCfg != nil {
		np = bsc.Filesystem.NumProcessorsPerCPU
	}
	return int(math.Ceil(float64(runtime.NumCPU()) * np))
}

// TODO: Remove once v1 endpoint no longer required.
func (bsc BootstrapConfiguration) peersFetchBlocksMetadataEndpointVersion() client.FetchBlocksMetadataEndpointVersion {
	version := client.FetchBlocksMetadataEndpointDefault
	if peersCfg := bsc.Peers; peersCfg != nil {
		version = peersCfg.FetchBlocksMetadataEndpointVersion
	}
	return version
}

// BootstrapFilesystemConfiguration specifies config for the fs bootstrapper.
type BootstrapFilesystemConfiguration struct {
	// NumProcessorsPerCPU is the number of processors per CPU.
	NumProcessorsPerCPU float64 `yaml:"numProcessorsPerCPU" validate:"min=0.0"`
}

// BootstrapPeersConfiguration specifies config for the peers bootstrapper.
type BootstrapPeersConfiguration struct {
	// FetchBlocksMetadataEndpointVersion is the endpoint to use when fetching blocks metadata.
	// TODO: Remove once v1 endpoint no longer required.
	FetchBlocksMetadataEndpointVersion client.FetchBlocksMetadataEndpointVersion `yaml:"fetchBlocksMetadataEndpointVersion"`
}

// New creates a bootstrap process based on the bootstrap configuration.
func (bsc BootstrapConfiguration) New(
	opts storage.Options,
	topoMapProvider topology.MapProvider,
	origin topology.Host,
	adminClient client.AdminClient,
) (bootstrap.ProcessProvider, error) {
	if err := ValidateBootstrappersOrder(bsc.Bootstrappers); err != nil {
		return nil, err
	}

	var (
		mutableSegmentAllocator = index.NewBootstrapResultMutableSegmentAllocator(
			opts.IndexOptions())
		bs  bootstrap.BootstrapperProvider
		err error
	)
	rsOpts := result.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions()).
		SetSeriesCachePolicy(opts.SeriesCachePolicy()).
		SetIndexMutableSegmentAllocator(mutableSegmentAllocator)

	fsOpts := opts.CommitLogOptions().FilesystemOptions()

	// Start from the end of the list because the bootstrappers are ordered by precedence in descending order.
	for i := len(bsc.Bootstrappers) - 1; i >= 0; i-- {
		switch bsc.Bootstrappers[i] {
		case bootstrapper.NoOpAllBootstrapperName:
			bs = bootstrapper.NewNoOpAllBootstrapperProvider()
		case bootstrapper.NoOpNoneBootstrapperName:
			bs = bootstrapper.NewNoOpNoneBootstrapperProvider()
		case bfs.FileSystemBootstrapperName:
			fsbOpts := bfs.NewOptions().
				SetInstrumentOptions(opts.InstrumentOptions()).
				SetResultOptions(rsOpts).
				SetFilesystemOptions(fsOpts).
				SetPersistManager(opts.PersistManager()).
				SetBoostrapDataNumProcessors(bsc.fsNumProcessors()).
				SetDatabaseBlockRetrieverManager(opts.DatabaseBlockRetrieverManager()).
				SetRuntimeOptionsManager(opts.RuntimeOptionsManager()).
				SetIdentifierPool(opts.IdentifierPool())
			bs, err = bfs.NewFileSystemBootstrapperProvider(fsbOpts, bs)
			if err != nil {
				return nil, err
			}
		case commitlog.CommitLogBootstrapperName:
			cOpts := commitlog.NewOptions().
				SetResultOptions(rsOpts).
				SetCommitLogOptions(opts.CommitLogOptions())

			inspection, err := fs.InspectFilesystem(fsOpts)
			if err != nil {
				return nil, err
			}
			bs, err = commitlog.NewCommitLogBootstrapperProvider(cOpts, inspection, bs)
			if err != nil {
				return nil, err
			}
		case peers.PeersBootstrapperName:
			pOpts := peers.NewOptions().
				SetResultOptions(rsOpts).
				SetAdminClient(adminClient).
				SetPersistManager(opts.PersistManager()).
				SetDatabaseBlockRetrieverManager(opts.DatabaseBlockRetrieverManager()).
				SetFetchBlocksMetadataEndpointVersion(bsc.peersFetchBlocksMetadataEndpointVersion()).
				SetRuntimeOptionsManager(opts.RuntimeOptionsManager())
			bs, err = peers.NewPeersBootstrapperProvider(pOpts, bs)
			if err != nil {
				return nil, err
			}
		case uninitialized.UninitializedTopologyBootstrapperName:
			uOpts := uninitialized.NewOptions().
				SetResultOptions(rsOpts).
				SetInstrumentOptions(opts.InstrumentOptions())
			bs = uninitialized.NewuninitializedTopologyBootstrapperProvider(uOpts, bs)
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

// ValidateBootstrappersOrder will validate that a list of bootstrappers specified
// is in valid order.
func ValidateBootstrappersOrder(names []string) error {
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
