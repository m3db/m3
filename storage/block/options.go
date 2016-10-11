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
		databaseBlockPool:       NewDatabaseBlockPool(nil),
		contextPool:             context.NewPool(nil, nil),
		encoderPool:             encoding.NewEncoderPool(nil),
		readerIteratorPool:      encoding.NewReaderIteratorPool(nil),
		multiReaderIteratorPool: encoding.NewMultiReaderIteratorPool(nil),
		segmentReaderPool:       xio.NewSegmentReaderPool(nil),
		bytesPool:               pool.NewBytesPool(nil, nil),
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

func (o *options) SetDatabaseBlockAllocSize(value int) Options {
	opts := *o
	opts.databaseBlockAllocSize = value
	return &opts
}

func (o *options) DatabaseBlockAllocSize() int {
	return o.databaseBlockAllocSize
}

func (o *options) SetDatabaseBlockPool(value DatabaseBlockPool) Options {
	opts := *o
	opts.databaseBlockPool = value
	return &opts
}

func (o *options) DatabaseBlockPool() DatabaseBlockPool {
	return o.databaseBlockPool
}

func (o *options) SetContextPool(value context.Pool) Options {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *options) ContextPool() context.Pool {
	return o.contextPool
}

func (o *options) SetEncoderPool(value encoding.EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) EncoderPool() encoding.EncoderPool {
	return o.encoderPool
}

func (o *options) SetReaderIteratorPool(value encoding.ReaderIteratorPool) Options {
	opts := *o
	opts.readerIteratorPool = value
	return &opts
}

func (o *options) ReaderIteratorPool() encoding.ReaderIteratorPool {
	return o.readerIteratorPool
}

func (o *options) SetMultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options {
	opts := *o
	opts.multiReaderIteratorPool = value
	return &opts
}

func (o *options) MultiReaderIteratorPool() encoding.MultiReaderIteratorPool {
	return o.multiReaderIteratorPool
}

func (o *options) SetSegmentReaderPool(value xio.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) SegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *options) SetBytesPool(value pool.BytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.BytesPool {
	return o.bytesPool
}
