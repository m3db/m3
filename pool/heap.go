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

package pool

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/uber-go/tally"
)

// NewNativeHeap constructs a new BytesPool based on NativePool.
func NewNativeHeap(b []Bucket, po ObjectPoolOptions) BytesPool {
	if po == nil {
		po = NewObjectPoolOptions()
	}

	var (
		m        = po.MetricsScope()
		ByteType = reflect.TypeOf((byte)(0))
	)

	sort.Sort(BucketByCapacity(b))

	r := heap{m: heapMetrics{
		overflows: m.Counter("overflows"),
		misplaces: m.Counter("misplaces"),
	}}

	for _, cfg := range b {
		m := m.SubScope(fmt.Sprintf("slot-%d", cfg.Capacity))

		v := &slot{class: cfg.Capacity, opts: NativePoolOptions{
			Size: uint(cfg.Count),
			Type: reflect.ArrayOf(cfg.Capacity, ByteType),
		}, m: slotMetrics{
			used: m.Gauge("used"),
			size: m.Gauge("size"),
		}}

		r.slots = append(r.slots, v)
	}

	return r
}

type heap struct {
	slots []*slot

	m heapMetrics
}

type heapMetrics struct {
	overflows tally.Counter
	misplaces tally.Counter
}

type slot struct {
	sync.RWMutex

	class int
	opts  NativePoolOptions
	pools []NativePool

	m slotMetrics
}

type slotMetrics struct {
	used tally.Gauge
	size tally.Gauge
}

func (s *slot) get() interface{} {
	if segment := s.getOr(s.RLocker(), func() interface{} {
		return nil
	}); segment != nil {
		return segment
	}

	// Slow path - double-check that there are no segments left,
	// then grow while holding an exclusive lock.
	return s.getOr(s, func() interface{} {
		s.pools = append(
			[]NativePool{NewNativePool(s.opts)}, s.pools...)
		return s.pools[0].Get()
	})
}

func (s *slot) getOr(l sync.Locker, fn OverflowFn) interface{} {
	var segment interface{}

	l.Lock()

	for _, pool := range s.pools {
		if segment = pool.GetOr(func() interface{} {
			return nil
		}); segment != nil {
			l.Unlock()
			s.updateMetrics()
			return segment
		}
	}

	segment = fn()

	l.Unlock()

	return segment
}

func (s *slot) put(segment interface{}) {
	s.RLock()

	for i := range s.pools {
		if s.pools[i].Owns(segment) {
			s.pools[i].Put(segment)
			break
		}
	}

	s.RUnlock()
}

func (s *slot) updateMetrics() {
	// TODO(@kobolog): Use Dice.
	if time.Now().UnixNano()%sampleObjectPoolLengthEvery != 0 {
		return
	}

	var used, size int64

	s.RLock()

	for i := range s.pools {
		a, b := s.pools[i].Size()

		used += int64(a)
		size += int64(b)
	}

	s.RUnlock()

	s.m.used.Update(used)
	s.m.size.Update(size)
}

func (p heap) Init() {
	for _, slot := range p.slots {
		slot.pools = append(slot.pools, NewNativePool(slot.opts))
	}
}

func (p heap) pick(class int, action func(*slot)) bool {
	for _, slot := range p.slots {
		if class <= slot.class {
			action(slot)
			return true
		}
	}

	return false
}

func (p heap) Get(n int) []byte {
	var head []byte

	if p.pick(n, func(slot *slot) {
		head = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
			Data: reflect.ValueOf(slot.get()).Pointer(),
			Len:  0,
			Cap:  slot.class}))
	}) {
		return head
	}

	p.m.overflows.Inc(1)

	// Allocate a segment directly from the system heap.
	return mmap(n)[:0:n]
}

func (p heap) Put(head []byte) {
	if p.pick(cap(head), func(slot *slot) {
		slot.put(unsafe.Pointer(
			(*reflect.SliceHeader)(unsafe.Pointer(&head)).Data))
	}) {
		return
	}

	// Nothing fits so it must be a system heap segment.
	if err := munmap(head[:cap(head)]); err != nil {
		p.m.misplaces.Inc(1)
	}
}
