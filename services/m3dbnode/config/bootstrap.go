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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3db/storage/bootstrap/result"
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
	adminClient client.AdminClient,
) (bootstrap.Process, error) {
	var (
		bs  bootstrap.Bootstrapper
		err error
	)

	rsopts := result.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions()).
		SetSeriesCachePolicy(opts.SeriesCachePolicy())

	// Start from the end of the list because the bootstrappers are ordered by precedence in descending order.
	for i := len(bsc.Bootstrappers) - 1; i >= 0; i-- {
		switch bsc.Bootstrappers[i] {
		case bootstrapper.NoOpAllBootstrapperName:
			bs = bootstrapper.NewNoOpAllBootstrapper()
		case bootstrapper.NoOpNoneBootstrapperName:
			bs = bootstrapper.NewNoOpNoneBootstrapper()
		case fs.FileSystemBootstrapperName:
			fsopts := opts.CommitLogOptions().FilesystemOptions()
			filePathPrefix := fsopts.FilePathPrefix()
			fsbopts := fs.NewOptions().
				SetResultOptions(rsopts).
				SetFilesystemOptions(fsopts).
				SetNumProcessors(bsc.fsNumProcessors()).
				SetDatabaseBlockRetrieverManager(opts.DatabaseBlockRetrieverManager())
			bs = fs.NewFileSystemBootstrapper(filePathPrefix, fsbopts, bs)
		case commitlog.CommitLogBootstrapperName:
			copts := commitlog.NewOptions().
				SetResultOptions(rsopts).
				SetCommitLogOptions(opts.CommitLogOptions())
			bs, err = commitlog.NewCommitLogBootstrapper(copts, bs)
			if err != nil {
				return nil, err
			}
		case peers.PeersBootstrapperName:
			popts := peers.NewOptions().
				SetResultOptions(rsopts).
				SetAdminClient(adminClient).
				SetPersistManager(opts.PersistManager()).
				SetDatabaseBlockRetrieverManager(opts.DatabaseBlockRetrieverManager()).
				SetFetchBlocksMetadataEndpointVersion(bsc.peersFetchBlocksMetadataEndpointVersion())
			bs, err = peers.NewPeersBootstrapper(popts, bs)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown bootstrapper: %s", bsc.Bootstrappers[i])
		}
	}

	return bootstrap.NewProcess(bs, rsopts), nil
}
