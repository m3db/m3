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

package storage

import (
	"io"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3ts"
	"github.com/m3db/m3db/encoding/tsz"
	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	xio "github.com/m3db/m3db/x/io"
)

const (
	// defaultCommitLogStrategy is the default commit log strategy
	defaultCommitLogStrategy = commitlog.StrategyWriteBehind

	// defaultMaxFlushRetries is the default number of retries when flush fails
	defaultMaxFlushRetries = 3
)

var (
	// defaultNewBootstrapFn is the default function for creating a new bootstrap
	defaultNewBootstrapFn = func() bootstrap.Bootstrap {
		return bootstrap.NewNoOpBootstrapProcess(bootstrap.NewOptions())
	}

	// defaultNewPersistManagerFn is the default function for creating a new persist manager
	defaultNewPersistManagerFn = func() persist.Manager {
		return fs.NewPersistManager(fs.NewOptions())
	}

	timeZero time.Time
)

type options struct {
	clockOpts               clock.Options
	instrumentOpts          instrument.Options
	retentionOpts           retention.Options
	blockOpts               block.Options
	commitLogOpts           commitlog.Options
	newEncoderFn            encoding.NewEncoderFn
	newDecoderFn            encoding.NewDecoderFn
	newBootstrapFn          NewBootstrapFn
	newPersistManagerFn     NewPersistManagerFn
	maxFlushRetries         int
	contextPool             context.Pool
	bytesPool               pool.BytesPool
	encoderPool             encoding.EncoderPool
	segmentReaderPool       xio.SegmentReaderPool
	readerIteratorPool      encoding.ReaderIteratorPool
	multiReaderIteratorPool encoding.MultiReaderIteratorPool
}

// NewOptions creates a new set of storage options with defaults
// TODO(r): add an "IsValid()" method and ensure buffer future and buffer past are
// less than blocksize and check when opening database
func NewOptions() Options {
	o := &options{
		clockOpts:               clock.NewOptions(),
		instrumentOpts:          instrument.NewOptions(),
		retentionOpts:           retention.NewOptions(),
		blockOpts:               block.NewOptions(),
		commitLogOpts:           commitlog.NewOptions(),
		newBootstrapFn:          defaultNewBootstrapFn,
		newPersistManagerFn:     defaultNewPersistManagerFn,
		maxFlushRetries:         defaultMaxFlushRetries,
		contextPool:             context.NewPool(0),
		bytesPool:               pool.NewBytesPool(nil),
		encoderPool:             encoding.NewEncoderPool(0),
		segmentReaderPool:       xio.NewSegmentReaderPool(0),
		readerIteratorPool:      encoding.NewReaderIteratorPool(0),
		multiReaderIteratorPool: encoding.NewMultiReaderIteratorPool(0),
	}
	return o.EncodingTszPooled()
}

func (o *options) ClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) GetClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) InstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) GetInstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) RetentionOptions(value retention.Options) Options {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *options) GetRetentionOptions() retention.Options {
	return o.retentionOpts
}

func (o *options) DatabaseBlockOptions(value block.Options) Options {
	opts := *o
	opts.blockOpts = value
	return &opts
}

func (o *options) GetDatabaseBlockOptions() block.Options {
	return o.blockOpts
}

func (o *options) CommitLogOptions(value commitlog.Options) Options {
	opts := *o
	opts.commitLogOpts = value
	return &opts
}

func (o *options) GetCommitLogOptions() commitlog.Options {
	return o.commitLogOpts
}

func (o *options) EncodingTszPooled() Options {
	return o.encodingPooled(false)
}

func (o *options) EncodingTsz() Options {
	return o.encoding(false)
}

func (o *options) EncodingM3tsPooled() Options {
	return o.encodingPooled(true)
}

func (o *options) EncodingM3ts() Options {
	return o.encoding(true)
}

func (o *options) encodingPooled(useM3ts bool) Options {
	opts := *o

	// NB(r): don't enable pooling just yet
	buckets := []pool.Bucket{}
	bytesPool := pool.NewBytesPool(buckets)
	bytesPool.Init()
	opts.bytesPool = bytesPool

	// initialize context pool
	contextPool := context.NewPool(0)
	opts.contextPool = contextPool

	// initialize segment reader pool
	segmentReaderPool := xio.NewSegmentReaderPool(0)
	opts.segmentReaderPool = segmentReaderPool

	encoderPool := encoding.NewEncoderPool(0)
	readerIteratorPool := encoding.NewReaderIteratorPool(0)
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(0)

	encodingOpts := encoding.NewOptions().
		BytesPool(bytesPool).
		EncoderPool(encoderPool).
		ReaderIteratorPool(readerIteratorPool).
		SegmentReaderPool(segmentReaderPool)

	// initialize encoder pool
	if useM3ts {
		encoderPool.Init(func() encoding.Encoder {
			return m3ts.NewEncoder(timeZero, nil, encodingOpts)
		})
	} else {
		encoderPool.Init(func() encoding.Encoder {
			return tsz.NewEncoder(timeZero, nil, encodingOpts)
		})
	}
	opts.encoderPool = encoderPool

	// initialize single reader iterator pool
	if useM3ts {
		readerIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
			return m3ts.NewReaderIterator(r, encodingOpts)
		})
	} else {
		readerIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
			return tsz.NewReaderIterator(r, encodingOpts)
		})
	}
	opts.readerIteratorPool = readerIteratorPool

	// initialize multi reader iterator pool
	if useM3ts {
		multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
			return m3ts.NewReaderIterator(r, encodingOpts)
		})
	} else {
		multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
			return tsz.NewReaderIterator(r, encodingOpts)
		})
	}
	opts.multiReaderIteratorPool = multiReaderIteratorPool

	opts.blockOpts = opts.blockOpts.EncoderPool(encoderPool)
	return (&opts).encoding(useM3ts)
}

func (o *options) encoding(useM3ts bool) Options {
	opts := *o
	encodingOpts := encoding.NewOptions()

	if useM3ts {
		newEncoderFn := func(start time.Time, bytes []byte) encoding.Encoder {
			return m3ts.NewEncoder(start, bytes, encodingOpts)
		}
		opts.newEncoderFn = newEncoderFn

		newDecoderFn := func() encoding.Decoder {
			return m3ts.NewDecoder(encodingOpts)
		}
		opts.newDecoderFn = newDecoderFn
		return &opts
	}

	newEncoderFn := func(start time.Time, bytes []byte) encoding.Encoder {
		return tsz.NewEncoder(start, bytes, encodingOpts)
	}
	opts.newEncoderFn = newEncoderFn

	newDecoderFn := func() encoding.Decoder {
		return tsz.NewDecoder(encodingOpts)
	}
	opts.newDecoderFn = newDecoderFn

	return &opts
}

func (o *options) NewEncoderFn(value encoding.NewEncoderFn) Options {
	opts := *o
	opts.newEncoderFn = value
	return &opts
}

func (o *options) GetNewEncoderFn() encoding.NewEncoderFn {
	return o.newEncoderFn
}

func (o *options) NewDecoderFn(value encoding.NewDecoderFn) Options {
	opts := *o
	opts.newDecoderFn = value
	return &opts
}

func (o *options) GetNewDecoderFn() encoding.NewDecoderFn {
	return o.newDecoderFn
}

func (o *options) NewBootstrapFn(value NewBootstrapFn) Options {
	opts := *o
	opts.newBootstrapFn = value
	return &opts
}

func (o *options) GetNewBootstrapFn() NewBootstrapFn {
	return o.newBootstrapFn
}

func (o *options) NewPersistManagerFn(value NewPersistManagerFn) Options {
	opts := *o
	opts.newPersistManagerFn = value
	return &opts
}

func (o *options) GetNewPersistManagerFn() NewPersistManagerFn {
	return o.newPersistManagerFn
}

func (o *options) MaxFlushRetries(value int) Options {
	opts := *o
	opts.maxFlushRetries = value
	return &opts
}

func (o *options) GetMaxFlushRetries() int {
	return o.maxFlushRetries
}

func (o *options) ContextPool(value context.Pool) Options {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *options) GetContextPool() context.Pool {
	return o.contextPool
}

func (o *options) BytesPool(value pool.BytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) GetBytesPool() pool.BytesPool {
	return o.bytesPool
}

func (o *options) EncoderPool(value encoding.EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) GetEncoderPool() encoding.EncoderPool {
	return o.encoderPool
}

func (o *options) SegmentReaderPool(value xio.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) GetSegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *options) ReaderIteratorPool(value encoding.ReaderIteratorPool) Options {
	opts := *o
	opts.readerIteratorPool = value
	return &opts
}

func (o *options) GetReaderIteratorPool() encoding.ReaderIteratorPool {
	return o.readerIteratorPool
}

func (o *options) MultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options {
	opts := *o
	opts.multiReaderIteratorPool = value
	return &opts
}

func (o *options) GetMultiReaderIteratorPool() encoding.MultiReaderIteratorPool {
	return o.multiReaderIteratorPool
}
