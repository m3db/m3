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

package cm

import "sync"

// StreamPool is a pool of streams, wrapping sync.Pool.
type StreamPool struct {
	pool *sync.Pool
}

// NewStreamPool creates a new StreamPool.
func NewStreamPool(opts Options) StreamPool {
	return StreamPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return NewStream(opts)
			},
		},
	}
}

// Get returns a new Stream from the pool.
func (p StreamPool) Get() *Stream {
	return p.pool.Get().(*Stream) //nolint:errcheck
}

// Put puts a Stream back into the pool.
func (p StreamPool) Put(s *Stream) {
	p.pool.Put(s)
}
