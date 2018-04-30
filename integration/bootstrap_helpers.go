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
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
)

// nolint: deadcode
func newTestBootstrapperSource(opts testBootstrapperSourceOptions, resultOpts result.Options, next bootstrap.Bootstrapper) bootstrap.Bootstrapper {
	src := testBootstrapperSource{}
	if opts.can != nil {
		src.can = opts.can
	} else {
		src.can = func(bootstrap.Strategy) bool { return true }
	}

	if opts.available != nil {
		src.available = opts.available
	} else {
		src.available = func(ns namespace.Metadata, shardsTimeRanges result.ShardTimeRanges) result.ShardTimeRanges {
			return shardsTimeRanges
		}
	}

	if opts.read != nil {
		src.read = opts.read
	} else {
		src.read = func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.BootstrapResult, error) {
			return result.NewBootstrapResult(), nil
		}
	}

	b := &testBootstrapper{}
	b.Bootstrapper = bootstrapper.NewBaseBootstrapper(src.String(), src, resultOpts, next)
	return b
}

type testBootstrapper struct {
	bootstrap.Bootstrapper
}

type testBootstrapperSourceOptions struct {
	can       func(bootstrap.Strategy) bool
	available func(namespace.Metadata, result.ShardTimeRanges) result.ShardTimeRanges
	read      func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.BootstrapResult, error)
}

type testBootstrapperSource struct {
	can       func(bootstrap.Strategy) bool
	available func(namespace.Metadata, result.ShardTimeRanges) result.ShardTimeRanges
	read      func(namespace.Metadata, result.ShardTimeRanges, bootstrap.RunOptions) (result.BootstrapResult, error)
}

func (t testBootstrapperSource) Can(strategy bootstrap.Strategy) bool {
	return t.can(strategy)
}

func (t testBootstrapperSource) Available(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	return t.available(ns, shardsTimeRanges)
}

func (t testBootstrapperSource) Read(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	return t.read(ns, shardsTimeRanges, opts)
}

func (t testBootstrapperSource) String() string {
	return "test-bootstrapper"
}
