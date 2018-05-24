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

package msg

import (
	"sync"

	"github.com/m3db/m3msg/producer"

	"go.uber.org/atomic"
)

// OnFinalizeFn will be called when the message is being finalized.
type OnFinalizeFn func(rm producer.RefCountedMessage)

type refCountedMessage struct {
	sync.RWMutex
	producer.Message

	onFinalizeFn OnFinalizeFn

	refCount            *atomic.Int32
	isDroppedOrConsumed *atomic.Bool
}

// NewRefCountedMessage creates RefCountedMessage.
func NewRefCountedMessage(m producer.Message, fn OnFinalizeFn) producer.RefCountedMessage {
	return &refCountedMessage{
		Message:             m,
		refCount:            atomic.NewInt32(0),
		onFinalizeFn:        fn,
		isDroppedOrConsumed: atomic.NewBool(false),
	}
}

func (rm *refCountedMessage) Accept(fn producer.FilterFunc) bool {
	return fn(rm.Message)
}

func (rm *refCountedMessage) IncRef() {
	rm.refCount.Inc()
}

func (rm *refCountedMessage) DecRef() {
	rc := rm.refCount.Dec()
	if rc == 0 {
		rm.finalize(producer.Consumed)
	}
	if rc < 0 {
		panic("invalid ref count")
	}
}

func (rm *refCountedMessage) IncReads() {
	rm.RLock()
}

func (rm *refCountedMessage) DecReads() {
	rm.RUnlock()
}

func (rm *refCountedMessage) Bytes() []byte {
	return rm.Message.Bytes()
}

func (rm *refCountedMessage) Size() uint64 {
	return uint64(rm.Message.Size())
}

func (rm *refCountedMessage) Drop() bool {
	return rm.finalize(producer.Dropped)
}

func (rm *refCountedMessage) IsDroppedOrConsumed() bool {
	return rm.isDroppedOrConsumed.Load()
}

func (rm *refCountedMessage) finalize(r producer.FinalizeReason) bool {
	rm.Lock()
	if rm.isDroppedOrConsumed.Load() {
		rm.Unlock()
		return false
	}
	rm.isDroppedOrConsumed.Store(true)
	rm.Unlock()
	if rm.onFinalizeFn != nil {
		rm.onFinalizeFn(rm)
	}
	rm.Message.Finalize(r)
	return true
}
