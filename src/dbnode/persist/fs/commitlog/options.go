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
	"fmt"
	"runtime"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

const (
	// defaultStrategy is the default commit log write strategy
	defaultStrategy = StrategyWriteBehind

	// defaultFailureStrategy is the default commit log failure strategy
	defaultFailureStrategy = FailureStrategyPanic

	// defaultFlushInterval is the default commit log flush interval
	defaultFlushInterval = time.Second

	// defaultFlushSize is the default commit log flush size
	defaultFlushSize = 65536

	// defaultBlockSize is the default commit log block size
	defaultBlockSize = 15 * time.Minute

	// defaultReadConcurrency is the default read concurrency
	defaultReadConcurrency = 4

	// MaximumQueueSizeQueueChannelSizeRatio is the maximum ratio between the
	// backlog queue size and backlog queue channel size.
	MaximumQueueSizeQueueChannelSizeRatio = 8.0
)

var (
	// defaultBacklogQueueSize is the default commit log backlog queue size.
	defaultBacklogQueueSize = 1024 * runtime.GOMAXPROCS(0)

	// defaultBacklogQueueChannelSize is the default commit log backlog queue channel size.
	defaultBacklogQueueChannelSize = int(float64(defaultBacklogQueueSize) / MaximumQueueSizeQueueChannelSizeRatio)
)

var (
	errFlushIntervalNonNegative = errors.New("flush interval must be non-negative")
	errBlockSizePositive        = errors.New("block size must be a positive duration")
	errReadConcurrencyPositive  = errors.New("read concurrency must be a positive integer")
	errMissingFailureCallback   = errors.New("failure callback must be non-nil if FailureStrategyCallback is used")
)

// FailureCallback is used in the FailureStrategyCallback failure mode.
// If this function returns false, the error will be treated as fatal.
// This function MUST exit quickly
type FailureCallback func(err error) bool

type options struct {
	clockOpts               clock.Options
	instrumentOpts          instrument.Options
	blockSize               time.Duration
	fsOpts                  fs.Options
	strategy                Strategy
	flushSize               int
	flushInterval           time.Duration
	backlogQueueSize        int
	backlogQueueChannelSize int
	bytesPool               pool.CheckedBytesPool
	identPool               ident.Pool
	readConcurrency         int
	failureMode             FailureStrategy
	failureCallback         FailureCallback
}

// the bools allow explicitly setting the field to nil
type optionsInput struct {
	fsOptions       fs.Options
	fsOptionsSet    bool

	identPoolOpts  ident.PoolOptions
}
type OptionSetter func(o *optionsInput)

func WithFileSystemOptions(o fs.Options) OptionSetter {
	return OptionSetter(func(input *optionsInput) {
		input.fsOptions = o
		input.fsOptionsSet = true
	})
}

func WithIdentPoolOptions(o ident.PoolOptions) OptionSetter {
	return OptionSetter(func(input *optionsInput) {
		input.identPoolOpts = o
	})
}


// NewOptions creates new commit log options
func NewOptions(setters ...OptionSetter) Options {
	input := optionsInput{}
	for _, setter := range setters {
		setter(&input)
	}

	if !input.fsOptionsSet && input.fsOptions == nil {
		input.fsOptions = fs.NewOptions()
	}

	o := &options{
		clockOpts:               clock.NewOptions(),
		instrumentOpts:          instrument.NewOptions(),
		blockSize:               defaultBlockSize,
		fsOpts:                  input.fsOptions,
		strategy:                defaultStrategy,
		failureMode:             defaultFailureStrategy,
		flushSize:               defaultFlushSize,
		flushInterval:           defaultFlushInterval,
		backlogQueueSize:        defaultBacklogQueueSize,
		backlogQueueChannelSize: defaultBacklogQueueChannelSize,
		bytesPool: pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, nil)
		}),
		readConcurrency: defaultReadConcurrency,
		failureCallback: nil,
	}

	o.bytesPool.Init()
	o.identPool = ident.NewPool(o.bytesPool, input.identPoolOpts)
	return o
}

func (o *options) Validate() error {
	if o.FlushInterval() < 0 {
		return errFlushIntervalNonNegative
	}

	if o.BlockSize() <= 0 {
		return errBlockSizePositive
	}

	if o.ReadConcurrency() <= 0 {
		return errReadConcurrencyPositive
	}

	if float64(o.BacklogQueueSize())/float64(o.BacklogQueueChannelSize()) > MaximumQueueSizeQueueChannelSizeRatio {
		return fmt.Errorf(
			"BacklogQueueSize / BacklogQueueChannelSize ratio must be at most: %f, but was: %f",
			MaximumQueueSizeQueueChannelSizeRatio, float64(o.BacklogQueueSize())/float64(o.BacklogQueueChannelSize()))
	}

	if o.FailureStrategy() == FailureStrategyCallback && o.FailureCallback() == nil {
		return errMissingFailureCallback
	}

	return nil
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

func (o *options) SetBlockSize(value time.Duration) Options {
	opts := *o
	opts.blockSize = value
	return &opts
}

func (o *options) BlockSize() time.Duration {
	return o.blockSize
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

func (o *options) SetBacklogQueueChannelSize(value int) Options {
	opts := *o
	opts.backlogQueueChannelSize = value
	return &opts
}

func (o *options) BacklogQueueChannelSize() int {
	return o.backlogQueueChannelSize
}

func (o *options) SetBytesPool(value pool.CheckedBytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *options) SetReadConcurrency(concurrency int) Options {
	opts := *o
	opts.readConcurrency = concurrency
	return &opts
}

func (o *options) ReadConcurrency() int {
	return o.readConcurrency
}

func (o *options) SetIdentifierPool(value ident.Pool) Options {
	opts := *o
	opts.identPool = value
	return &opts
}

func (o *options) IdentifierPool() ident.Pool {
	return o.identPool
}

func (o *options) SetFailureStrategy(value FailureStrategy) Options {
	opts := *o
	opts.failureMode = value
	return &opts
}

func (o *options) FailureStrategy() FailureStrategy {
	return o.failureMode
}

func (o *options) SetFailureCallback(value FailureCallback) Options {
	opts := *o
	opts.failureCallback = value
	return &opts
}

func (o *options) FailureCallback() FailureCallback {
	return o.failureCallback
}
