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

package producer

import (
	"sync"

	"go.uber.org/atomic"
)

// OnFinalizeFn will be called when the message is being finalized.
type OnFinalizeFn func(rm *RefCountedMessage)

// RefCountedMessage is a reference counted message.
type RefCountedMessage struct {
	Message
	// RefCountedMessage must not be copied by value due to RWMutex,
	// safe to store values here and not just pointers
	onFinalizeFn        OnFinalizeFn
	size                uint64
	refCount            atomic.Int32
	isDroppedOrConsumed atomic.Bool
	mu                  sync.RWMutex
}

// NewRefCountedMessage creates RefCountedMessage.
func NewRefCountedMessage(m Message, fn OnFinalizeFn) *RefCountedMessage {
	if fn == nil {
		// in non-test code the finalizer is always set.
		fn = noopFinalizer
	}
	return &RefCountedMessage{
		Message:      m,
		size:         uint64(m.Size()),
		onFinalizeFn: fn,
	}
}

// Accept returns true if the message can be accepted by the filter.
func (rm *RefCountedMessage) Accept(fn FilterFunc) bool {
	return fn(rm.Message)
}

// IncRef increments the ref count.
func (rm *RefCountedMessage) IncRef() {
	rm.refCount.Inc()
}

// DecRef decrements the ref count. If the reference count became zero after
// the call, the message will be finalized as consumed.
func (rm *RefCountedMessage) DecRef() {
	rc := rm.refCount.Dec()
	if rc == 0 {
		rm.finalize(Consumed)
	}
	if rc < 0 {
		panic("invalid ref count")
	}
}

// IncReads increments the reads count.
func (rm *RefCountedMessage) IncReads() {
	rm.mu.RLock()
}

// DecReads decrements the reads count.
func (rm *RefCountedMessage) DecReads() {
	rm.mu.RUnlock()
}

// NumRef returns the number of references remaining.
func (rm *RefCountedMessage) NumRef() int32 {
	return rm.refCount.Load()
}

// Size returns the size of the message.
func (rm *RefCountedMessage) Size() uint64 {
	return rm.size
}

// Drop drops the message without waiting for it to be consumed.
func (rm *RefCountedMessage) Drop() bool {
	return rm.finalize(Dropped)
}

// IsDroppedOrConsumed returns true if the message has been dropped or consumed.
func (rm *RefCountedMessage) IsDroppedOrConsumed() bool {
	return rm.isDroppedOrConsumed.Load()
}

func (rm *RefCountedMessage) finalize(r FinalizeReason) bool {
	// NB: This lock prevents the message from being finalized when its still
	// being read.
	rm.mu.Lock()
	if rm.isDroppedOrConsumed.Load() {
		rm.mu.Unlock()
		return false
	}
	rm.isDroppedOrConsumed.Store(true)
	rm.mu.Unlock()

	rm.onFinalizeFn(rm)
	rm.Message.Finalize(r)
	return true
}

func noopFinalizer(rm *RefCountedMessage) {}
