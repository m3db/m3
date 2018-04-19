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

package writer

import "github.com/m3db/m3x/pool"

// messagePool is the pool for message.
type messagePool interface {
	// Init initializes the pool.
	Init()

	// Get gets a message from the pool.
	Get() *message

	// Put puts a message to the pool.
	Put(m *message)
}

type msgPool struct {
	pool.ObjectPool
}

// TODO: Remove the nolint comment after adding usage of this function.
// nolint: deadcode
func newMessagePool(opts pool.ObjectPoolOptions) messagePool {
	p := pool.NewObjectPool(opts)
	return &msgPool{p}
}

func (p *msgPool) Init() {
	p.ObjectPool.Init(func() interface{} {
		return newMessage()
	})
}

func (p *msgPool) Get() *message {
	return p.ObjectPool.Get().(*message)
}

func (p *msgPool) Put(m *message) {
	p.ObjectPool.Put(m)
}
