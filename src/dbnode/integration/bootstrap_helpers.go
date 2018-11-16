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

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	bcl "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"

	"github.com/stretchr/testify/require"
)

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

var _ bootstrap.Source = &testBootstrapperSource{}

type testBootstrapperSource struct {
	can            func(bootstrap.Strategy) bool
	availableData  func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.ShardTimeRanges, error)
	readData       func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.DataBootstrapResult, error)
	availableIndex func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.ShardTimeRanges, error)
	readIndex      func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.IndexBootstrapResult, error)
}

func (t testBootstrapperSource) Can(strategy bootstrap.Strategy) bool {
	return t.can(strategy)
}

func (t testBootstrapperSource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return t.availableData(ns, shardsTimeRanges, runOpts)
}

func (t testBootstrapperSource) ReadData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	return t.readData(ns, shardsTimeRanges, opts)
}

func (t testBootstrapperSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return t.availableIndex(ns, shardsTimeRanges, runOpts)
}

func (t testBootstrapperSource) ReadIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	return t.readIndex(ns, shardsTimeRanges, opts)
}

func (t testBootstrapperSource) String() string {
	return "test-bootstrapper"
}

func setupCommitLogBootstrapperWithFSInspection(
	t *testing.T, setup *testSetup, commitLogOpts commitlog.Options) {
	noOpAll := bootstrapper.NewNoOpAllBootstrapperProvider()
	bsOpts := newDefaulTestResultOptions(setup.storageOpts)
	bclOpts := bcl.NewOptions().
		SetResultOptions(bsOpts).
		SetCommitLogOptions(commitLogOpts).
		SetRuntimeOptionsManager(runtime.NewOptionsManager())
	fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
	bs, err := bcl.NewCommitLogBootstrapperProvider(
		bclOpts, mustInspectFilesystem(fsOpts), noOpAll)
	require.NoError(t, err)
	processOpts := bootstrap.NewProcessOptions().
		SetTopologyMapProvider(setup).
		SetOrigin(setup.origin)
	process, err := bootstrap.NewProcessProvider(bs, processOpts, bsOpts)
	require.NoError(t, err)
	setup.storageOpts = setup.storageOpts.SetBootstrapProcessProvider(process)
}

func mustInspectFilesystem(fsOpts fs.Options) fs.Inspection {
	inspection, err := fs.InspectFilesystem(fsOpts)
	if err != nil {
		panic(err)
	}

	return inspection
}
