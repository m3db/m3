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

package agent

import (
	"github.com/m3db/m3/src/m3nsch"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"
)

var (
	// avg latency per m3db write op is ~1 ms when the CPU is under load
	// so liberally, we set MaxWorkerQPS at ~ 10K writes per sec
	defaultMaxWorkerQPS = int64(10000)

	// defaultConcurrency is the default number of go routines used during
	// load generation
	defaultConcurrency = int(2000)

	// defaultTimeUnit is the default unit of time used during load operations
	defaultTimeUnit = xtime.Second
)

type agentOpts struct {
	iopts        instrument.Options
	maxWorkerQPS int64
	concurrency  int
	newSessionFn m3nsch.NewSessionFn
	timeUnit     xtime.Unit
}

// NewOptions returns a new AgentOptions object with default values
func NewOptions(
	iopts instrument.Options,
) m3nsch.AgentOptions {
	return &agentOpts{
		iopts:        iopts,
		maxWorkerQPS: defaultMaxWorkerQPS,
		concurrency:  defaultConcurrency,
		timeUnit:     defaultTimeUnit,
	}
}

func (so *agentOpts) SetInstrumentOptions(iopts instrument.Options) m3nsch.AgentOptions {
	so.iopts = iopts
	return so
}

func (so *agentOpts) InstrumentOptions() instrument.Options {
	return so.iopts
}

func (so *agentOpts) SetMaxWorkerQPS(qps int64) m3nsch.AgentOptions {
	so.maxWorkerQPS = qps
	return so
}

func (so *agentOpts) MaxWorkerQPS() int64 {
	return so.maxWorkerQPS
}

func (so *agentOpts) SetConcurrency(c int) m3nsch.AgentOptions {
	so.concurrency = c
	return so
}

func (so *agentOpts) Concurrency() int {
	return so.concurrency
}

func (so *agentOpts) SetNewSessionFn(fn m3nsch.NewSessionFn) m3nsch.AgentOptions {
	so.newSessionFn = fn
	return so
}

func (so *agentOpts) NewSessionFn() m3nsch.NewSessionFn {
	return so.newSessionFn
}

func (so *agentOpts) SetTimeUnit(u xtime.Unit) m3nsch.AgentOptions {
	so.timeUnit = u
	return so
}

func (so *agentOpts) TimeUnit() xtime.Unit {
	return so.timeUnit
}
