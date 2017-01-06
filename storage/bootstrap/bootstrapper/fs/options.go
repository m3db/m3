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

package fs

import (
	"math"
	"runtime"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/bootstrap/result"
)

const (
	defaultNumIOWorkers = 1
)

var (
	defaultNumProcessors = int(math.Ceil(float64(runtime.NumCPU()) / 2))
)

type options struct {
	resultOpts    result.Options
	fsOpts        fs.Options
	numIOWorkers  int
	numProcessors int
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		resultOpts:    result.NewOptions(),
		fsOpts:        fs.NewOptions(),
		numIOWorkers:  defaultNumIOWorkers,
		numProcessors: defaultNumProcessors,
	}
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOpts = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOpts
}

func (o *options) SetFilesystemOptions(value fs.Options) Options {
	opts := *o
	opts.fsOpts = value
	return &opts
}

func (o *options) FilesystemOptions() fs.Options {
	return o.fsOpts
}

func (o *options) SetNumIOWorkers(value int) Options {
	opts := *o
	opts.numIOWorkers = value
	return &opts
}

func (o *options) NumIOWorkers() int {
	return o.numIOWorkers
}

func (o *options) SetNumProcessors(value int) Options {
	opts := *o
	opts.numProcessors = value
	return &opts
}

func (o *options) NumProcessors() int {
	return o.numProcessors
}
