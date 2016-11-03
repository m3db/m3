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
	"reflect"
	"sort"
	"sync"
	"unsafe"
)

// NewNativeHeap constructs a new BytesPool based on NativePool.
func NewNativeHeap(sizes []Bucket) BytesPool {
	var (
		slots = make([]*slot, len(sizes))
		T     = reflect.TypeOf((byte)(0))
	)

	sort.Sort(BucketByCapacity(sizes))

	for i, cfg := range sizes {
		ns := &slot{class: cfg.Capacity, cfg: NativePoolOptions{
			Size: uint(cfg.Count),
			Type: reflect.ArrayOf(cfg.Capacity, T)}}
		slots[i] = ns
	}

	return heap(slots)
}

type heap []*slot

type slot struct {
	sync.RWMutex

	class int
	cfg   NativePoolOptions
	pools []NativePool
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
			[]NativePool{NewNativePool(s.cfg)}, s.pools...)
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

func (p heap) Init() {
	for _, slot := range p {
		slot.pools = append(slot.pools, NewNativePool(slot.cfg))
	}
}

func (p heap) pick(class int, action func(*slot)) bool {
	for _, slot := range p {
		if class <= slot.class {
			action(slot)
			return true
		}
	}

	return false
}

func (p heap) Get(n int) []byte {
	var head []byte

	if !p.pick(n, func(slot *slot) {
		head = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
			Data: reflect.ValueOf(slot.get()).Pointer(),
			Len:  0,
			Cap:  slot.class}))
	}) {
		// Allocate a segment directly from the system heap.
		head = mmap(n)[0:0:n]
	}

	return head
}

func (p heap) Put(head []byte) {
	if !p.pick(cap(head), func(slot *slot) {
		slot.put(unsafe.Pointer(
			(*reflect.SliceHeader)(unsafe.Pointer(&head)).Data))
	}) {
		// Nothing fits so it must be a system heap segment.
		munmap(head[:cap(head)])
	}
}
