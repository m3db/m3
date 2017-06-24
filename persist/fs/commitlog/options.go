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
	"fmt"
	"runtime"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

const (
	// defaultStrategy is the default commit log write strategy
	defaultStrategy = StrategyWriteBehind

	// defaultFlushSize is the default commit log flush size
	defaultFlushSize = 65536

	// defaultFlushInterval is the default commit log flush interval
	defaultFlushInterval = time.Second
)

var (
	// defaultBacklogQueueSize is the default commit log backlog queue size
	defaultBacklogQueueSize = 1024 * runtime.NumCPU()
)

type options struct {
	clockOpts        clock.Options
	instrumentOpts   instrument.Options
	retentionOpts    retention.Options
	fsOpts           fs.Options
	strategy         Strategy
	flushSize        int
	flushInterval    time.Duration
	backlogQueueSize int
	bytesPool        pool.CheckedBytesPool
}

// NewOptions creates new commit log options
func NewOptions() Options {
	o := &options{
		clockOpts:        clock.NewOptions(),
		instrumentOpts:   instrument.NewOptions(),
		retentionOpts:    retention.NewOptions(),
		fsOpts:           fs.NewOptions(),
		strategy:         defaultStrategy,
		flushSize:        defaultFlushSize,
		flushInterval:    defaultFlushInterval,
		backlogQueueSize: defaultBacklogQueueSize,
		bytesPool: pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, nil)
		}),
	}
	o.bytesPool.Init()
	return o
}

func (o *options) Validate() error {
	var multiErr xerrors.MultiError
	if o.FlushInterval() < 0 {
		multiErr = multiErr.Add(
			fmt.Errorf("flush interval must non-negative"))
	}

	// TODO(pratek): does strategy/flush size need to be checked here

	if err := o.retentionOpts.Validate(); err != nil {
		multiErr = multiErr.Add(
			fmt.Errorf("invalid commit log retention options: %v", err))
	}

	fsOpts := o.FilesystemOptions()
	if err := fsOpts.Validate(); err != nil {
		multiErr = multiErr.Add(
			fmt.Errorf("invalid commit log fs options: %v", err))
	}

	// short circuit checking detailed errors per namespace until
	// top level objects are OK
	if err := multiErr.FinalError(); err != nil {
		return err
	}

	blockSize := o.retentionOpts.BlockSize()
	retentionPeriod := o.retentionOpts.RetentionPeriod()

	// NB(prateek): ensure all namespaces in the registry have retention options
	// compatible with the commit log retention options. i.e. these constraints are met:
	//
	//   (1) no namespace block size is smaller than commit log block size
	//   (2) all namespace block sizes are divisible by the commit log block size
	// Rationale: together these properties make it easier to reason about which commit
	// log block can be removed, or is needed to bootstrap a namespace.
	//
	//   (3) no namespace retention period is larger than commit log retention period
	// Rationale: in the worst case, we would have no fileset files, and would need to
	// re-read the entire history of operations off the commit log. This requires we
	// retain commit logs for the entire retention period of all namespaces. We can obviously
	// delete the commit log blocks once we have fileset files covering the corresponding blocks.
	//
	// That said, this is probably too theorectical a condition for our usecase. We should just
	// mandate a minimum retention of 1-2 days for the commit log and live with what holes arise
	// after that.
	metadatas := fsOpts.NamespaceRegistry().Metadatas()
	for _, md := range metadatas {
		id := md.ID()
		ropts := md.Options().RetentionOptions()

		nsBlockSize := ropts.BlockSize()
		if nsBlockSize < blockSize {
			multiErr = multiErr.Add(fmt.Errorf(
				"namespace %s has a block size [%v] smaller than that of the commit log [%v]",
				id.String(), nsBlockSize.String(), blockSize.String()))
		}

		if mod := nsBlockSize.Nanoseconds() % blockSize.Nanoseconds(); mod != 0 {
			multiErr = multiErr.Add(fmt.Errorf(
				"namespace %s has a block size [%v] not divisible by that of the commit log [%v]",
				id.String(), nsBlockSize.String(), blockSize.String()))
		}

		nsRetentionPeriod := ropts.RetentionPeriod()
		if nsRetentionPeriod > retentionPeriod {
			multiErr = multiErr.Add(fmt.Errorf(
				"namespace %s has a retention period [%v] larger than that of the commit log [%v]",
				id.String(), nsRetentionPeriod.String(), retentionPeriod.String()))
		}
	}

	return multiErr.FinalError()
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetRetentionOptions(value retention.Options) Options {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *options) RetentionOptions() retention.Options {
	return o.retentionOpts
}

func (o *options) SetFilesystemOptions(value fs.Options) Options {
	opts := *o
	opts.fsOpts = value
	return &opts
}

func (o *options) FilesystemOptions() fs.Options {
	return o.fsOpts
}

func (o *options) SetStrategy(value Strategy) Options {
	opts := *o
	opts.strategy = value
	return &opts
}

func (o *options) Strategy() Strategy {
	return o.strategy
}

func (o *options) SetFlushSize(value int) Options {
	opts := *o
	opts.flushSize = value
	return &opts
}

func (o *options) FlushSize() int {
	return o.flushSize
}

func (o *options) SetFlushInterval(value time.Duration) Options {
	opts := *o
	opts.flushInterval = value
	return &opts
}

func (o *options) FlushInterval() time.Duration {
	return o.flushInterval
}

func (o *options) SetBacklogQueueSize(value int) Options {
	opts := *o
	opts.backlogQueueSize = value
	return &opts
}

func (o *options) BacklogQueueSize() int {
	return o.backlogQueueSize
}

func (o *options) SetBytesPool(value pool.CheckedBytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}
