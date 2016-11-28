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
	"unsafe"
)

// NativePoolOptions specify options for NativePool.
type NativePoolOptions struct {
	Construct func(ptr interface{})
	Size      uint
	Type      reflect.Type
}

// NativePool represents an object pool which is opaque to the Go GC.
type NativePool interface {
	Get() interface{}
	GetOr(OverflowFn) interface{}
	Put(interface{})

	// Owns determines if the object belongs to the pool.
	Owns(interface{}) bool

	// Size returns the available and the total capacity of the pool.
	Size() (uint64, uint64)
}

// OverflowFn produces non-pooled objects.
type OverflowFn func() interface{}

// NewNativePool constructs a new NativePool.
func NewNativePool(opts NativePoolOptions) NativePool {
	p := &nativePool{
		free: make(chan uint64, opts.Size),
		opts: opts,
		// TODO(@kobolog): alignment & padding.
		step: uint64(hsz + opts.Type.Size())}

	p.size = uint64(p.opts.Size) * p.step

	if p.size == 0 {
		panic("native-pool: pool size is zero")
	}

	if p.step%uint64(p.opts.Type.Align()) != 0 {
		panic("native-pool: invalid alignment")
	}

	if p.opts.Construct == nil {
		p.opts.Construct = func(interface{}) {}
	}

	p.init()

	return p
}

type hdr struct {
	// Offset from the beginning of the arena to the object, we chose
	// to use uint64 to address > 4GB and for the hdr structure to be
	// aligned at the largest possible value.
	idx uint64
}

// Header size with padding, as determined by the compiler.
const hsz = unsafe.Sizeof(*(*hdr)(nil))

type nativePool struct {
	pool       []byte
	free       chan uint64
	opts       NativePoolOptions
	step, size uint64
}

func (p *nativePool) init() {
	// Heap is a slice of bytes large enough to fit opts.Size objects
	// of type struct { hdr; T }.
	if r, err := mmap(int(p.size)); err != nil {
		panic("mmap() error: " + err.Error())
	} else {
		p.pool = r
	}

	for i := uint64(0); i < p.size; i += p.step {
		hdr := (*hdr)(unsafe.Pointer(&p.pool[i]))
		hdr.idx = i + uint64(hsz)
		ptr := unsafe.Pointer(&p.pool[hdr.idx])

		p.opts.Construct(reflect.NewAt(p.opts.Type, ptr).Interface())

		// Alternatively, we can use a channel of interfaces, but it
		// would keep opts.Size objects rooted in the channel.
		// We aim to avoid allocating unnecessary GC-visible objects.
		p.free <- hdr.idx
	}
}

func (p *nativePool) Size() (uint64, uint64) {
	return uint64(len(p.free)) * p.step, p.size
}

// Get provides an object from the pool.
func (p *nativePool) Get() interface{} {
	return reflect.NewAt(
		p.opts.Type, unsafe.Pointer(&p.pool[<-p.free])).Interface()
}

func (p *nativePool) GetOr(fn OverflowFn) interface{} {
	select {
	case next := <-p.free:
		return reflect.NewAt(
			p.opts.Type, unsafe.Pointer(&p.pool[next])).Interface()
	default:
		return fn()
	}
}

// Put returns an object to the pool.
func (p *nativePool) Put(object interface{}) {
	ptr := unsafe.Pointer(reflect.ValueOf(object).Pointer())

	if !p.owns(ptr) {
		return
	}

	// We know that this object is in our arena, so it's okay
	// to read memory directly in front of it.
	p.free <- (*hdr)(unsafe.Pointer(uintptr(ptr) - hsz)).idx
}

func (p *nativePool) owns(addr unsafe.Pointer) bool {
	if (uintptr(addr) >= uintptr(unsafe.Pointer(&p.pool[0]))) &&
		uintptr(addr) <= uintptr(unsafe.Pointer(&p.pool[p.size-1])) {
		return true
	}

	return false
}

func (p *nativePool) Owns(object interface{}) bool {
	return p.owns(unsafe.Pointer(reflect.ValueOf(object).Pointer()))
}
