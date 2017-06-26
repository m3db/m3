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

package series

import (
	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

type options struct {
	clockOpts                     clock.Options
	instrumentOpts                instrument.Options
	retentionOpts                 retention.Options
	blockOpts                     block.Options
	contextPool                   context.Pool
	encoderPool                   encoding.EncoderPool
	multiReaderIteratorPool       encoding.MultiReaderIteratorPool
	fetchBlockMetadataResultsPool block.FetchBlockMetadataResultsPool
	identifierPool                ts.IdentifierPool
}

// NewOptions creates new database series options
func NewOptions() Options {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{
		pool.Bucket{Count: 4096, Capacity: 128},
	}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	return &options{
		clockOpts:                     clock.NewOptions(),
		instrumentOpts:                instrument.NewOptions(),
		retentionOpts:                 retention.NewOptions(),
		blockOpts:                     block.NewOptions(),
		contextPool:                   context.NewPool(nil, nil),
		encoderPool:                   encoding.NewEncoderPool(nil),
		multiReaderIteratorPool:       encoding.NewMultiReaderIteratorPool(nil),
		fetchBlockMetadataResultsPool: block.NewFetchBlockMetadataResultsPool(nil, 0),
		identifierPool:                ts.NewIdentifierPool(bytesPool, nil),
	}
}

func (o *options) Validate() error {
	return o.retentionOpts.Validate()
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

func (o *options) SetDatabaseBlockOptions(value block.Options) Options {
	opts := *o
	opts.blockOpts = value
	return &opts
}

func (o *options) DatabaseBlockOptions() block.Options {
	return o.blockOpts
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

func (o *options) SetMultiReaderIteratorPool(value encoding.MultiReaderIteratorPool) Options {
	opts := *o
	opts.multiReaderIteratorPool = value
	return &opts
}

func (o *options) MultiReaderIteratorPool() encoding.MultiReaderIteratorPool {
	return o.multiReaderIteratorPool
}

func (o *options) SetFetchBlockMetadataResultsPool(value block.FetchBlockMetadataResultsPool) Options {
	opts := *o
	opts.fetchBlockMetadataResultsPool = value
	return &opts
}

func (o *options) FetchBlockMetadataResultsPool() block.FetchBlockMetadataResultsPool {
	return o.fetchBlockMetadataResultsPool
}

func (o *options) SetIdentifierPool(value ts.IdentifierPool) Options {
	opts := *o
	opts.identifierPool = value
	return &opts
}

func (o *options) IdentifierPool() ts.IdentifierPool {
	return o.identifierPool
}
