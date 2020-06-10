// +build integration

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

package integration

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	bcl "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/x/context"

	"github.com/stretchr/testify/require"
)

func newTestBootstrapperSource(
	opts testBootstrapperSourceOptions,
	resultOpts result.Options,
	next bootstrap.Bootstrapper,
) bootstrap.BootstrapperProvider {
	src := testBootstrapperSource{}

	if opts.availableData != nil {
		src.availableData = opts.availableData
	} else {
		src.availableData = func(_ namespace.Metadata, shardsTimeRanges result.ShardTimeRanges, _ bootstrap.RunOptions) (result.ShardTimeRanges, error) {
			return shardsTimeRanges, nil
		}
	}

	if opts.availableIndex != nil {
		src.availableIndex = opts.availableIndex
	} else {
		src.availableIndex = func(_ namespace.Metadata, shardsTimeRanges result.ShardTimeRanges, _ bootstrap.RunOptions) (result.ShardTimeRanges, error) {
			return shardsTimeRanges, nil
		}
	}

	if opts.read != nil {
		src.read = opts.read
	} else {
		src.read = func(ctx context.Context, namespaces bootstrap.Namespaces) (bootstrap.NamespaceResults, error) {
			return bootstrap.NewNamespaceResults(namespaces), nil
		}
	}

	var (
		b   = &testBootstrapper{}
		err error
	)
	b.Bootstrapper, err = bootstrapper.NewBaseBootstrapper(src.String(), src, resultOpts, next)
	if err != nil {
		panic(err)
	}
	return testBootstrapperProvider{Bootstrapper: b}
}

var _ bootstrap.BootstrapperProvider = &testBootstrapperProvider{}

type testBootstrapperProvider struct {
	bootstrap.Bootstrapper
}

func (p testBootstrapperProvider) String() string {
	return p.Bootstrapper.String()
}

func (p testBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	return p.Bootstrapper, nil
}

type testBootstrapper struct {
	bootstrap.Bootstrapper
}

type testBootstrapperSourceOptions struct {
	availableData  func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.ShardTimeRanges, error)
	availableIndex func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.ShardTimeRanges, error)
	read           func(ctx context.Context, namespaces bootstrap.Namespaces) (bootstrap.NamespaceResults, error)
}

var _ bootstrap.Source = &testBootstrapperSource{}

type testBootstrapperSource struct {
	availableData  func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.ShardTimeRanges, error)
	availableIndex func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.ShardTimeRanges, error)
	read           func(ctx context.Context, namespaces bootstrap.Namespaces) (bootstrap.NamespaceResults, error)
}

func (t testBootstrapperSource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return t.availableData(ns, shardsTimeRanges, runOpts)
}

func (t testBootstrapperSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return t.availableIndex(ns, shardsTimeRanges, runOpts)
}

func (t testBootstrapperSource) Read(
	ctx context.Context,
	namespaces bootstrap.Namespaces,
) (bootstrap.NamespaceResults, error) {
	return t.read(ctx, namespaces)
}

func (t testBootstrapperSource) String() string {
	return "test-bootstrapper"
}

func setupCommitLogBootstrapperWithFSInspection(
	t *testing.T, setup TestSetup, commitLogOpts commitlog.Options) {
	noOpAll := bootstrapper.NewNoOpAllBootstrapperProvider()
	bsOpts := newDefaulTestResultOptions(setup.StorageOpts())
	bclOpts := bcl.NewOptions().
		SetResultOptions(bsOpts).
		SetCommitLogOptions(commitLogOpts).
		SetRuntimeOptionsManager(runtime.NewOptionsManager())
	fsOpts := setup.StorageOpts().CommitLogOptions().FilesystemOptions()
	bs, err := bcl.NewCommitLogBootstrapperProvider(
		bclOpts, mustInspectFilesystem(fsOpts), noOpAll)
	require.NoError(t, err)
	processOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(setup).
		SetOrigin(setup.Origin())
	process, err := bootstrap.NewProcessProvider(bs, processOpts, bsOpts)
	require.NoError(t, err)
	setup.SetStorageOpts(setup.StorageOpts().SetBootstrapProcessProvider(process))
}

func mustInspectFilesystem(fsOpts fs.Options) fs.Inspection {
	inspection, err := fs.InspectFilesystem(fsOpts)
	if err != nil {
		panic(err)
	}

	return inspection
}
