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

package data

import (
	"sync"

	"github.com/m3db/m3msg/producer"

	"go.uber.org/atomic"
)

// OnFinalizeFn will be called when the data is being finalized.
type OnFinalizeFn func(d producer.RefCountedData)

type refCountedData struct {
	sync.RWMutex
	producer.Data

	onFinalizeFn OnFinalizeFn

	refCount            *atomic.Int32
	isDroppedOrConsumed *atomic.Bool
}

// NewRefCountedData creates RefCountedData.
func NewRefCountedData(data producer.Data, fn OnFinalizeFn) producer.RefCountedData {
	return &refCountedData{
		Data:                data,
		refCount:            atomic.NewInt32(0),
		onFinalizeFn:        fn,
		isDroppedOrConsumed: atomic.NewBool(false),
	}
}

func (d *refCountedData) Accept(fn producer.FilterFunc) bool {
	return fn(d.Data)
}

func (d *refCountedData) IncRef() {
	d.refCount.Inc()
}

func (d *refCountedData) DecRef() {
	rc := d.refCount.Dec()
	if rc == 0 {
		d.finalize(producer.Consumed)
	}
	if rc < 0 {
		panic("invalid ref count")
	}
}

func (d *refCountedData) IncReads() {
	d.RLock()
}

func (d *refCountedData) DecReads() {
	d.RUnlock()
}

func (d *refCountedData) Bytes() []byte {
	return d.Data.Bytes()
}

func (d *refCountedData) Size() uint64 {
	return uint64(d.Data.Size())
}

func (d *refCountedData) Drop() bool {
	return d.finalize(producer.Dropped)
}

func (d *refCountedData) IsDroppedOrConsumed() bool {
	return d.isDroppedOrConsumed.Load()
}

func (d *refCountedData) finalize(r producer.FinalizeReason) bool {
	d.Lock()
	if d.isDroppedOrConsumed.Load() {
		d.Unlock()
		return false
	}
	d.isDroppedOrConsumed.Store(true)
	d.Unlock()
	if d.onFinalizeFn != nil {
		d.onFinalizeFn(d)
	}
	d.Data.Finalize(r)
	return true
}
