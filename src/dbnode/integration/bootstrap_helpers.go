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
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
)

func newTestBootstrapperSource(
	opts testBootstrapperSourceOptions,
	resultOpts result.Options,
	next bootstrap.Bootstrapper,
) bootstrap.BootstrapperProvider {
	src := testBootstrapperSource{}
	if opts.can != nil {
		src.can = opts.can
	} else {
		src.can = func(bootstrap.Strategy) bool { return true }
	}

	if opts.availableData != nil {
		src.availableData = opts.availableData
	} else {
		src.availableData = func(ns namespace.Metadata, shardsTimeRanges result.ShardTimeRanges) result.ShardTimeRanges {
			return shardsTimeRanges
		}
	}

	if opts.readData != nil {
		src.readData = opts.readData
	} else {
		src.readData = func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.DataBootstrapResult, error) {
			return result.NewDataBootstrapResult(), nil
		}
	}

	if opts.availableIndex != nil {
		src.availableIndex = opts.availableIndex
	} else {
		src.availableIndex = func(ns namespace.Metadata, shardsTimeRanges result.ShardTimeRanges) result.ShardTimeRanges {
			return shardsTimeRanges
		}
	}

	if opts.readIndex != nil {
		src.readIndex = opts.readIndex
	} else {
		src.readIndex = func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.IndexBootstrapResult, error) {
			return result.NewIndexBootstrapResult(), nil
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
	can            func(bootstrap.Strategy) bool
	availableData  func(namespace.Metadata, result.ShardTimeRanges) result.ShardTimeRanges
	readData       func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.DataBootstrapResult, error)
	availableIndex func(namespace.Metadata, result.ShardTimeRanges) result.ShardTimeRanges
	readIndex      func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.IndexBootstrapResult, error)
}

var _ bootstrap.Source = &testBootstrapperSource{}

type testBootstrapperSource struct {
	can            func(bootstrap.Strategy) bool
	availableData  func(namespace.Metadata, result.ShardTimeRanges) result.ShardTimeRanges
	readData       func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.DataBootstrapResult, error)
	availableIndex func(namespace.Metadata, result.ShardTimeRanges) result.ShardTimeRanges
	readIndex      func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.IndexBootstrapResult, error)
}

func (t testBootstrapperSource) Can(strategy bootstrap.Strategy) bool {
	return t.can(strategy)
}

func (t testBootstrapperSource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	return t.availableData(ns, shardsTimeRanges)
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
) result.ShardTimeRanges {
	return t.availableIndex(ns, shardsTimeRanges)
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
