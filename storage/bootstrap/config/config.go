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
	"fmt"
	"math"
	"runtime"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3db/storage/bootstrap/result"
)

var (
	// defaultNumProcessors is the default number of processors
	defaultNumProcessors = int(math.Ceil(float64(runtime.NumCPU()) / 2))
)

type filesystemConfiguration struct {
	// NumProcessorsPerCPU is the number of processors per cpu
	NumProcessorsPerCPU float64 `yaml:"numProcessorsPerCPU" validate:"min=0.0"`
}

// Configuration captures the configuration for bootstrappers.
type Configuration struct {
	// Bootstrappers is the list of bootstrappers, ordered by precedence in descending order
	Bootstrappers []string `yaml:"bootstrappers" validate:"nonzero"`

	// Filesystem bootstrapper configuration
	FilesystemConfiguration *filesystemConfiguration `yaml:"fs"`
}

func (bsc Configuration) numProcessors() int {
	if bsc.FilesystemConfiguration == nil {
		return defaultNumProcessors
	}

	numProcessorsPerCPU := bsc.FilesystemConfiguration.NumProcessorsPerCPU
	if numProcessorsPerCPU <= 0.0 {
		return defaultNumProcessors
	}

	return int(math.Ceil(float64(runtime.NumCPU()) * numProcessorsPerCPU))
}

// New creates a bootstrap process based on the bootstrap configuration.
func (bsc Configuration) New(
	opts storage.Options,
	adminClient client.AdminClient,
	blockRetrieverMgr block.DatabaseBlockRetrieverManager,
) (bootstrap.Process, error) {
	var bs bootstrap.Bootstrapper

	rsopts := result.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetRetentionOptions(opts.RetentionOptions()).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions())

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
				SetNumProcessors(bsc.numProcessors()).
				SetDatabaseBlockRetrieverManager(blockRetrieverMgr)
			bs = fs.NewFileSystemBootstrapper(filePathPrefix, fsbopts, bs)
		case commitlog.CommitLogBootstrapperName:
			copts := commitlog.NewOptions().
				SetResultOptions(rsopts).
				SetCommitLogOptions(opts.CommitLogOptions())
			bs = commitlog.NewCommitLogBootstrapper(copts, bs)
		case peers.PeersBootstrapperName:
			popts := peers.NewOptions().
				SetResultOptions(rsopts).
				SetAdminClient(adminClient).
				SetPersistManager(opts.PersistManager()).
				SetDatabaseBlockRetrieverManager(blockRetrieverMgr)
			bs = peers.NewPeersBootstrapper(popts, bs)
		default:
			return nil, fmt.Errorf("unknown bootstrapper name %s", bsc.Bootstrappers[i])
		}
	}

	return bootstrap.NewProcess(bs, rsopts), nil
}
