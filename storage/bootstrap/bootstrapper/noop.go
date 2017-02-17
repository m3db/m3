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
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
)

const (
	// NoOpNoneBootstrapperName is the name of the noOpNoneBootstrapper
	NoOpNoneBootstrapperName = "noop-none"

	// NoOpAllBootstrapperName is the name of the noOpAllBootstrapper
	NoOpAllBootstrapperName = "noop-all"
)

var (
	defaultNoOpNoneBootstrapper = &noOpNoneBootstrapper{}
	defaultNoOpAllBootstrapper  = &noOpAllBootstrapper{}
)

// noOpNoneBootstrapper is the no-op bootstrapper that doesn't
// know how to bootstrap any time ranges.
type noOpNoneBootstrapper struct{}

// NewNoOpNoneBootstrapper creates a new noOpNoneBootstrapper.
func NewNoOpNoneBootstrapper() bootstrap.Bootstrapper {
	return defaultNoOpNoneBootstrapper
}

func (noop *noOpNoneBootstrapper) Can(strategy bootstrap.Strategy) bool {
	return true
}

func (noop *noOpNoneBootstrapper) Bootstrap(
	_ ts.ID,
	targetRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	res := result.NewBootstrapResult()
	res.SetUnfulfilled(targetRanges)
	return res, nil
}

func (noop *noOpNoneBootstrapper) String() string {
	return NoOpNoneBootstrapperName
}

// noOpAllBootstrapper is the no-op bootstrapper that pretends
// it can bootstrap any time ranges.
type noOpAllBootstrapper struct{}

// NewNoOpAllBootstrapper creates a new noOpAllBootstrapper.
func NewNoOpAllBootstrapper() bootstrap.Bootstrapper {
	return defaultNoOpAllBootstrapper
}

func (noop *noOpAllBootstrapper) Can(strategy bootstrap.Strategy) bool {
	return true
}

func (noop *noOpAllBootstrapper) Bootstrap(
	_ ts.ID,
	_ result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	return result.NewBootstrapResult(), nil
}

func (noop *noOpAllBootstrapper) String() string {
	return NoOpAllBootstrapperName
}
