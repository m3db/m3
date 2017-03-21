// Copyright (c) 2017 Uber Technologies, Inc.
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

package coordinator

import (
	"time"

	"github.com/m3db/m3nsch"
	"github.com/m3db/m3x/instrument"
)

var (
	defaultRPCTimeout  = time.Minute
	defaultParallelOps = true
)

type coordinatorOpts struct {
	iopts    instrument.Options
	timeout  time.Duration
	parallel bool
}

// NewOptions creates a new options struct.
func NewOptions(
	iopts instrument.Options,
) m3nsch.CoordinatorOptions {
	return &coordinatorOpts{
		iopts:    iopts,
		timeout:  defaultRPCTimeout,
		parallel: defaultParallelOps,
	}
}

func (mo *coordinatorOpts) SetInstrumentOptions(iopts instrument.Options) m3nsch.CoordinatorOptions {
	mo.iopts = iopts
	return mo
}

func (mo *coordinatorOpts) InstrumentOptions() instrument.Options {
	return mo.iopts
}

func (mo *coordinatorOpts) SetTimeout(d time.Duration) m3nsch.CoordinatorOptions {
	mo.timeout = d
	return mo
}

func (mo *coordinatorOpts) Timeout() time.Duration {
	return mo.timeout
}

func (mo *coordinatorOpts) SetParallelOperations(f bool) m3nsch.CoordinatorOptions {
	mo.parallel = f
	return mo
}

func (mo *coordinatorOpts) ParallelOperations() bool {
	return mo.parallel
}
