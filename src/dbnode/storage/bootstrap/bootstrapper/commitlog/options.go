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

package commitlog

import (
	"errors"
	"math"
	goruntime "runtime"

	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
)

const (
	// DefaultReturnUnfulfilledForCorruptCommitLogFiles is the default
	// value for whether to return unfulfilled when encountering corrupt
	// commit log files.
	DefaultReturnUnfulfilledForCorruptCommitLogFiles = false
)

var (
	errAccumulateConcurrencyPositive = errors.New("accumulate concurrency must be positive")
	errRuntimeOptsMgrNotSet          = errors.New("runtime options manager is not set")

	// defaultAccumulateConcurrency determines how fast to accumulate results.
	defaultAccumulateConcurrency = int(math.Max(float64(goruntime.GOMAXPROCS(0))*0.75, 1))
)

type options struct {
	resultOpts                                result.Options
	commitLogOpts                             commitlog.Options
	accumulateConcurrency                     int
	runtimeOptsMgr                            runtime.OptionsManager
	returnUnfulfilledForCorruptCommitLogFiles bool
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		resultOpts:            result.NewOptions(),
		commitLogOpts:         commitlog.NewOptions(),
		accumulateConcurrency: defaultAccumulateConcurrency,
		returnUnfulfilledForCorruptCommitLogFiles: DefaultReturnUnfulfilledForCorruptCommitLogFiles,
	}
}

func (o *options) Validate() error {
	if o.accumulateConcurrency <= 0 {
		return errAccumulateConcurrencyPositive
	}
	if o.runtimeOptsMgr == nil {
		return errRuntimeOptsMgrNotSet
	}
	return o.commitLogOpts.Validate()
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOpts = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOpts
}

func (o *options) SetCommitLogOptions(value commitlog.Options) Options {
	opts := *o
	opts.commitLogOpts = value
	return &opts
}

func (o *options) CommitLogOptions() commitlog.Options {
	return o.commitLogOpts
}

func (o *options) SetAccumulateConcurrency(value int) Options {
	opts := *o
	opts.accumulateConcurrency = value
	return &opts
}

func (o *options) AccumulateConcurrency() int {
	return o.accumulateConcurrency
}

func (o *options) SetRuntimeOptionsManager(value runtime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsMgr = value
	return &opts
}

func (o *options) RuntimeOptionsManager() runtime.OptionsManager {
	return o.runtimeOptsMgr
}

func (o *options) SetReturnUnfulfilledForCorruptCommitLogFiles(value bool) Options {
	opts := *o
	opts.returnUnfulfilledForCorruptCommitLogFiles = value
	return &opts
}

func (o *options) ReturnUnfulfilledForCorruptCommitLogFiles() bool {
	return o.returnUnfulfilledForCorruptCommitLogFiles
}
