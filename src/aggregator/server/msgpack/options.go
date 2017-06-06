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

package msgpack

import (
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

// Options provide a set of server options
type Options interface {
	// SetInstrumentOptions sets the instrument options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options
	InstrumentOptions() instrument.Options

	// SetRetryOptions sets the retry options for accepting connections
	SetRetryOptions(value xretry.Options) Options

	// RetryOptions returns the retry options for accepting connections
	RetryOptions() xretry.Options

	// SetIteratorPool sets the iterator pool
	SetIteratorPool(value msgpack.UnaggregatedIteratorPool) Options

	// IteratorPool returns the iterator pool
	IteratorPool() msgpack.UnaggregatedIteratorPool
}

type options struct {
	instrumentOpts instrument.Options
	retryOpts      xretry.Options
	iteratorPool   msgpack.UnaggregatedIteratorPool
}

// NewOptions creates a new set of server options
func NewOptions() Options {
	iteratorPool := msgpack.NewUnaggregatedIteratorPool(nil)
	opts := msgpack.NewUnaggregatedIteratorOptions().SetIteratorPool(iteratorPool)
	iteratorPool.Init(func() msgpack.UnaggregatedIterator {
		return msgpack.NewUnaggregatedIterator(nil, opts)
	})

	return &options{
		instrumentOpts: instrument.NewOptions(),
		retryOpts:      xretry.NewOptions(),
		iteratorPool:   iteratorPool,
	}
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetRetryOptions(value xretry.Options) Options {
	opts := *o
	opts.retryOpts = value
	return &opts
}

func (o *options) RetryOptions() xretry.Options {
	return o.retryOpts
}

func (o *options) SetIteratorPool(value msgpack.UnaggregatedIteratorPool) Options {
	opts := *o
	opts.iteratorPool = value
	return &opts
}

func (o *options) IteratorPool() msgpack.UnaggregatedIteratorPool {
	return o.iteratorPool
}
