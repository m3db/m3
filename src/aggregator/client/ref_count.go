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

package client

import "sync/atomic"

type destructorFn func()

type refCount struct {
	destructorFn destructorFn
	n            int32
}

func (rc *refCount) SetRefCount(n int)             { atomic.StoreInt32(&rc.n, int32(n)) }
func (rc *refCount) SetDestructor(fn destructorFn) { rc.destructorFn = fn }

func (rc *refCount) IncRef() int {
	if n := int(atomic.AddInt32(&rc.n, 1)); n > 0 {
		return n
	}
	panic("invalid ref count")
}

func (rc *refCount) DecRef() int {
	if n := int(atomic.AddInt32(&rc.n, -1)); n == 0 {
		if rc.destructorFn != nil {
			rc.destructorFn()
		}
		return n
	} else if n > 0 {
		return n
	}
	panic("invalid ref count")
}
