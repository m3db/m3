// Copyright (c) 2018 Uber Technologies, Inc.
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

package consumer

import (
	"github.com/m3db/m3x/pool"
)

// MessagePoolOptions are options to use when creating the
// message pool.
type MessagePoolOptions struct {
	PoolOptions pool.ObjectPoolOptions

	// MaxBufferReuseSize specifies the maximum buffer which can
	// be reused and pooled, if a buffer greater than this
	// is used then it is discarded. Zero specifies no limit.
	MaxBufferReuseSize int
}

type messagePool struct {
	pool.ObjectPool
	opts MessagePoolOptions
}

func newMessagePool(opts MessagePoolOptions) *messagePool {
	iOpts := opts.PoolOptions.InstrumentOptions()
	scope := iOpts.MetricsScope()
	return &messagePool{
		ObjectPool: pool.NewObjectPool(
			opts.PoolOptions.SetInstrumentOptions(
				iOpts.SetMetricsScope(
					scope.Tagged(map[string]string{"pool": "message"}),
				),
			),
		),
		opts: opts,
	}
}

func (p *messagePool) Init() {
	p.ObjectPool.Init(func() interface{} {
		return newMessage(p)
	})
}

func (p *messagePool) Get() *message {
	return p.ObjectPool.Get().(*message)
}

func (p *messagePool) Put(m *message) {
	max := p.opts.MaxBufferReuseSize
	if max > 0 && cap(m.Bytes()) > max {
		// Do not return to pool if enforcing max buffer reuse size
		return
	}
	p.ObjectPool.Put(m)
}
