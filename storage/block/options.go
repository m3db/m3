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

package block

import (
	"io"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/pool"
	xio "github.com/m3db/m3db/x/io"
)

const (
	// defaultDatabaseBlockAllocSize is the size to allocate for values for each
	// database block, this should match the size of expected encoded values per
	// block size.
	defaultDatabaseBlockAllocSize = 1024
)

type options struct {
	databaseBlockAllocSize  int
	databaseBlockPool       DatabaseBlockPool
	contextPool             context.Pool
	encoderPool             encoding.EncoderPool
	segmentReaderPool       xio.SegmentReaderPool
	bytesPool               pool.BytesPool
	readerIteratorPool      encoding.ReaderIteratorPool
	multiReaderIteratorPool encoding.MultiReaderIteratorPool
}

// NewOptions creates new database block options
func NewOptions() Options {
	o := &options{
		databaseBlockAllocSize:  defaultDatabaseBlockAllocSize,
		databaseBlockPool:       NewDatabaseBlockPool(0),
		contextPool:             context.NewPool(0),
		encoderPool:             encoding.NewEncoderPool(0),
		readerIteratorPool:      encoding.NewReaderIteratorPool(0),
		multiReaderIteratorPool: encoding.NewMultiReaderIteratorPool(0),
		segmentReaderPool:       xio.NewSegmentReaderPool(0),
		bytesPool:               pool.NewBytesPool(nil),
	}
	o.databaseBlockPool.Init(func() DatabaseBlock {
		return NewDatabaseBlock(timeZero, nil, o)
	})
	o.encoderPool.Init(func() encoding.Encoder {
		return encoding.NewNullEncoder()
	})
	o.readerIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		it := encoding.NewNullReaderIterator()
		it.Reset(r)
		return it
	})
	o.multiReaderIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		it := o.readerIteratorPool.Get()
		it.Reset(r)
		return it
	})
	return o
}

func (o *options) DatabaseBlockAllocSize(value int) Options {
	opts := *o
	opts.databaseBlockAllocSize = value
	return &opts
}

func (o *options) GetDatabaseBlockAllocSize() int {
	return o.databaseBlockAllocSize
}

func (o *options) DatabaseBlockPool(value DatabaseBlockPool) Options {
	opts := *o
	opts.databaseBlockPool = value
	return &opts
}

func (o *options) GetDatabaseBlockPool() DatabaseBlockPool {
	return o.databaseBlockPool
}

func (o *options) ContextPool(value context.Pool) Options {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *options) GetContextPool() context.Pool {
	return o.contextPool
}

func (o *options) EncoderPool(value encoding.EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) GetEncoderPool() encoding.EncoderPool {
	return o.encoderPool
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

func (o *options) SegmentReaderPool(value xio.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) GetSegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *options) BytesPool(value pool.BytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) GetBytesPool() pool.BytesPool {
	return o.bytesPool
}
