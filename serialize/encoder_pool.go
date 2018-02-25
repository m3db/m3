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

package serialize

import "github.com/m3db/m3x/pool"

type encoderPool struct {
	pool            pool.ObjectPool
	initEncoderSize int
}

func NewEncoderPool(initEncoderSize int, opts pool.ObjectPoolOptions) EncoderPool {
	pool := pool.NewObjectPool(opts)
	return &encoderPool{
		pool:            pool,
		initEncoderSize: initEncoderSize,
	}
}

func (p *encoderPool) Init() {
	p.pool.Init(func() interface{} {
		return newEncoder(p.initEncoderSize, p)
	})
}

func (p *encoderPool) Get() Encoder {
	return p.pool.Get().(Encoder)
}

func (p *encoderPool) Put(e Encoder) {
	p.pool.Put(e)
}
