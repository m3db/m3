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

package msgpack

import "github.com/m3db/m3x/clock"

// placementOptions provide a set of placement options.
type placementOptions interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) placementOptions

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetHashGenFn sets the hash generating function.
	SetHashGenFn(value HashGenFn) placementOptions

	// HashGenFn returns the hash generating function.
	HashGenFn() HashGenFn

	// SetInstanceWriterManager sets the instance writer manager.
	SetInstanceWriterManager(value instanceWriterManager) placementOptions

	// InstanceWriterManager returns the instance writer manager.
	InstanceWriterManager() instanceWriterManager
}

type placementOpts struct {
	clockOpts clock.Options
	hashGenFn HashGenFn
	writerMgr instanceWriterManager
}

func newPlacementOptions() placementOptions {
	return &placementOpts{
		clockOpts: clock.NewOptions(),
		hashGenFn: defaultHashGen,
	}
}

func (o *placementOpts) SetClockOptions(value clock.Options) placementOptions {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *placementOpts) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *placementOpts) SetHashGenFn(value HashGenFn) placementOptions {
	opts := *o
	opts.hashGenFn = value
	return &opts
}

func (o *placementOpts) HashGenFn() HashGenFn {
	return o.hashGenFn
}

func (o *placementOpts) SetInstanceWriterManager(value instanceWriterManager) placementOptions {
	opts := *o
	opts.writerMgr = value
	return &opts
}

func (o *placementOpts) InstanceWriterManager() instanceWriterManager {
	return o.writerMgr
}
