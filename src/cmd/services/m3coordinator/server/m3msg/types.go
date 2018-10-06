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

package m3msg

import (
	"sync/atomic"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3msg/consumer"
)

// WriteFn is the function that writes a metric.
type WriteFn func(
	id []byte,
	metricTimeNanos int64,
	value float64,
	sp policy.StoragePolicy,
	callback *RefCountedCallback,
)

// CallbackType defines the type for the callback.
type CallbackType int

// Supported CallbackTypes.
const (
	OnError CallbackType = iota
	OnSuccess
)

// RefCountedCallback wraps a message with a reference count, the message will
// be acked once the reference count decrements to zero.
type RefCountedCallback struct {
	ref int32
	msg consumer.Message
}

// NewRefCountedCallback creates a RefCountedCallback.
func NewRefCountedCallback(msg consumer.Message) *RefCountedCallback {
	return &RefCountedCallback{
		ref: 0,
		msg: msg,
	}
}

// incRef increments the ref count.
func (r *RefCountedCallback) incRef() {
	atomic.AddInt32(&r.ref, 1)
}

// decRef decrements the ref count. If the reference count became zero after
// the call, the message will be acked.
func (r *RefCountedCallback) decRef() {
	ref := atomic.AddInt32(&r.ref, -1)
	if ref == 0 {
		r.msg.Ack()
		return
	}
	if ref < 0 {
		panic("invalid ref count")
	}
}

// Callback performs the callback.
func (r *RefCountedCallback) Callback(t CallbackType) {
	if t == OnSuccess {
		r.decRef()
	}
}
