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

package server

import (
	"math"
	"runtime"

	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

const (
	defaultPacketQueueSize = 4096
)

var (
	defaultWorkerPoolSize = int(math.Max(1.0, float64(runtime.NumCPU()/8)))
)

type options struct {
	instrumentOpts  instrument.Options
	retrier         xretry.Retrier
	iteratorPool    msgpack.UnaggregatedIteratorPool
	packetQueueSize int
	workerPoolSize  int
}

// NewOptions creates a new set of server options
func NewOptions() Options {
	iteratorPool := msgpack.NewUnaggregatedIteratorPool(nil)
	opts := msgpack.NewUnaggregatedIteratorOptions().SetIteratorPool(iteratorPool)
	iteratorPool.Init(func() msgpack.UnaggregatedIterator {
		return msgpack.NewUnaggregatedIterator(nil, opts)
	})

	return &options{
		instrumentOpts:  instrument.NewOptions(),
		retrier:         xretry.NewRetrier(xretry.NewOptions()),
		iteratorPool:    iteratorPool,
		packetQueueSize: defaultPacketQueueSize,
		workerPoolSize:  defaultWorkerPoolSize,
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

func (o *options) SetRetrier(value xretry.Retrier) Options {
	opts := *o
	opts.retrier = value
	return &opts
}

func (o *options) Retrier() xretry.Retrier {
	return o.retrier
}

func (o *options) SetIteratorPool(value msgpack.UnaggregatedIteratorPool) Options {
	opts := *o
	opts.iteratorPool = value
	return &opts
}

func (o *options) IteratorPool() msgpack.UnaggregatedIteratorPool {
	return o.iteratorPool
}

func (o *options) SetPacketQueueSize(value int) Options {
	opts := *o
	opts.packetQueueSize = value
	return &opts
}

func (o *options) PacketQueueSize() int {
	return o.packetQueueSize
}

func (o *options) SetWorkerPoolSize(value int) Options {
	opts := *o
	opts.workerPoolSize = value
	return &opts
}

func (o *options) WorkerPoolSize() int {
	return o.workerPoolSize
}
