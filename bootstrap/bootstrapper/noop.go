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
	"github.com/m3db/m3db/interfaces/m3db"

	xtime "github.com/m3db/m3db/x/time"
)

const (
	// noOpNoneBootstrapperName is the name of the noOpNoneBootstrapper
	noOpNoneBootstrapperName = "noop-none"

	// noOpAllBootstrapperName is the name of the noOpAllBootstrapper
	noOpAllBootstrapperName = "noop-all"
)

var (
	defaultNoOpNoneBootstrapper = &noOpNoneBootstrapper{}
	defaultNoOpAllBootstrapper  = &noOpAllBootstrapper{}
)

// noOpNoneBootstrapper is the no-op bootstrapper that doesn't
// know how to bootstrap any time ranges.
type noOpNoneBootstrapper struct{}

// NewNoOpNoneBootstrapper creates a new noOpNoneBootstrapper.
func NewNoOpNoneBootstrapper() m3db.Bootstrapper {
	return defaultNoOpNoneBootstrapper
}

func (noop *noOpNoneBootstrapper) Bootstrap(shard uint32, targetRanges xtime.Ranges) (m3db.ShardResult, xtime.Ranges) {
	return nil, targetRanges
}

func (noop *noOpNoneBootstrapper) String() string {
	return noOpNoneBootstrapperName
}

// noOpAllBootstrapper is the no-op bootstrapper that pretends
// it can bootstrap any time ranges.
type noOpAllBootstrapper struct{}

// NewNoOpAllBootstrapper creates a new noOpAllBootstrapper.
func NewNoOpAllBootstrapper() m3db.Bootstrapper {
	return defaultNoOpAllBootstrapper
}

func (noop *noOpAllBootstrapper) Bootstrap(shard uint32, targetRanges xtime.Ranges) (m3db.ShardResult, xtime.Ranges) {
	return nil, nil
}

func (noop *noOpAllBootstrapper) String() string {
	return noOpAllBootstrapperName
}
