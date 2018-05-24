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

package bootstrapper

import (
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
)

const (
	// NoOpNoneBootstrapperName is the name of the noOpNoneBootstrapper
	NoOpNoneBootstrapperName = "noop-none"

	// NoOpAllBootstrapperName is the name of the noOpAllBootstrapperProvider
	NoOpAllBootstrapperName = "noop-all"
)

// noOpNoneBootstrapperProvider is the no-op bootstrapper provider that doesn't
// know how to bootstrap any time ranges.
type noOpNoneBootstrapperProvider struct{}

// NewNoOpNoneBootstrapperProvider creates a new noOpNoneBootstrapper.
func NewNoOpNoneBootstrapperProvider() bootstrap.BootstrapperProvider {
	return noOpNoneBootstrapperProvider{}
}

func (noop noOpNoneBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	return noOpNoneBootstrapper{}, nil
}

func (noop noOpNoneBootstrapperProvider) String() string {
	return NoOpNoneBootstrapperName
}

// Compilation of this statement ensures struct implements the interface.
var _ bootstrap.Bootstrapper = noOpNoneBootstrapper{}

type noOpNoneBootstrapper struct{}

func (noop noOpNoneBootstrapper) String() string {
	return NoOpNoneBootstrapperName
}

func (noop noOpNoneBootstrapper) Can(strategy bootstrap.Strategy) bool {
	return true
}

func (noop noOpNoneBootstrapper) BootstrapData(
	_ namespace.Metadata,
	targetRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	res := result.NewDataBootstrapResult()
	res.SetUnfulfilled(targetRanges)
	return res, nil
}

func (noop noOpNoneBootstrapper) BootstrapIndex(
	ns namespace.Metadata,
	targetRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	res := result.NewIndexBootstrapResult()
	res.SetUnfulfilled(targetRanges)
	return res, nil
}

// noOpAllBootstrapperProvider is the no-op bootstrapper provider that pretends
// it can bootstrap any time ranges.
type noOpAllBootstrapperProvider struct{}

// NewNoOpAllBootstrapperProvider creates a new noOpAllBootstrapperProvider.
func NewNoOpAllBootstrapperProvider() bootstrap.BootstrapperProvider {
	return noOpAllBootstrapperProvider{}
}

func (noop noOpAllBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	return noOpAllBootstrapper{}, nil
}

func (noop noOpAllBootstrapperProvider) String() string {
	return NoOpAllBootstrapperName
}

// Compilation of this statement ensures struct implements the interface.
var _ bootstrap.Bootstrapper = noOpAllBootstrapper{}

type noOpAllBootstrapper struct{}

func (noop noOpAllBootstrapper) String() string {
	return NoOpAllBootstrapperName
}

func (noop noOpAllBootstrapper) Can(strategy bootstrap.Strategy) bool {
	return true
}

func (noop noOpAllBootstrapper) BootstrapData(
	_ namespace.Metadata,
	_ result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	return result.NewDataBootstrapResult(), nil
}

func (noop noOpAllBootstrapper) BootstrapIndex(
	_ namespace.Metadata,
	_ result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	return result.NewIndexBootstrapResult(), nil
}
