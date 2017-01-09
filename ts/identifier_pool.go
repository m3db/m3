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

package ts

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
)

// NewIdentifierPool constructs a new simple IdentifierPool.
func NewIdentifierPool(options pool.ObjectPoolOptions) IdentifierPool {
	p := &simpleIdentifierPool{pool: pool.NewObjectPool(options)}
	p.pool.Init(func() interface{} { return &id{pool: p} })

	return p
}

type simpleIdentifierPool struct {
	pool pool.ObjectPool
}

func (p *simpleIdentifierPool) GetBinaryID(ctx context.Context, v checked.Bytes) ID {
	id := p.pool.Get().(*id)
	ctx.RegisterFinalizer(id)

	v.IncRef()
	id.pool, id.data = p, v

	return id
}

func (p *simpleIdentifierPool) GetStringID(ctx context.Context, v string) ID {
	data := checked.NewBytes([]byte(v), nil)
	return p.GetBinaryID(ctx, data)
}

func (p *simpleIdentifierPool) Put(v ID) {
	v.Reset()
	p.pool.Put(v)
}

func (p *simpleIdentifierPool) Clone(existing ID) ID {
	id := p.pool.Get().(*id)

	data := existing.Data()
	data.IncRef()

	newData := checked.NewBytes(nil, nil)
	newData.IncRef()
	newData.AppendAll(data.Get())

	data.DecRef()

	id.pool, id.data = p, newData

	return id
}

// NewNativeIdentifierPool constructs a new NativeIdentifierPool.
func NewNativeIdentifierPool(
	heap pool.CheckedBytesPool,
	options pool.ObjectPoolOptions,
) IdentifierPool {

	if options == nil {
		options = pool.NewObjectPoolOptions()
	}

	size := options.Size()

	return &nativeIdentifierPool{
		pool: pool.NewNativePool(pool.NativePoolOptions{
			Type: reflect.TypeOf(id{}),
			Size: uint(size),
		}),
		heap: configureHeap(heap),
		refs: checkedBytesRefs{
			idRefsToBytesRefs: make(map[uintptr]unsafe.Pointer, size),
		},
	}
}

type nativeIdentifierPool struct {
	pool pool.NativePool
	heap pool.CheckedBytesPool
	refs checkedBytesRefs
}

type checkedBytesRefs struct {
	sync.Mutex
	idRefsToBytesRefs map[uintptr]unsafe.Pointer
}

func configureHeap(heap pool.CheckedBytesPool) pool.CheckedBytesPool {
	if heap != nil {
		return heap
	}

	b := []pool.Bucket{
		{Capacity: 128, Count: 4096},
		{Capacity: 256, Count: 2048},
	}

	p := pool.NewCheckedBytesPool(b, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewNativeHeap(s, nil)
	})
	p.Init()

	return p
}

func create() interface{} {
	return &id{}
}

// GetBinaryID returns a new ID based on a binary value.
func (p *nativeIdentifierPool) GetBinaryID(ctx context.Context, v checked.Bytes) ID {
	id := p.pool.GetOr(create).(*id)
	ctx.RegisterFinalizer(id)

	v.IncRef()
	id.pool, id.data = p, v

	return id
}

// GetStringID returns a new ID based on a string value.
func (p *nativeIdentifierPool) GetStringID(ctx context.Context, str string) ID {
	id := p.pool.GetOr(create).(*id)
	ctx.RegisterFinalizer(id)

	v := p.heapRetrieve(id, len(str))
	v.IncRef()
	v.AppendAll([]byte(str))

	id.pool, id.data = p, v

	return id
}

func (p *nativeIdentifierPool) Put(v ID) {
	v.Reset()
	id := v.(*id)
	p.heapRelease(id)
	p.pool.Put(id)
}

// Clone replicates given ID into a new ID from the pool.
func (p *nativeIdentifierPool) Clone(existing ID) ID {
	id := p.pool.GetOr(create).(*id)

	data := existing.Data()
	data.IncRef()

	v := p.heapRetrieve(id, data.Len())
	v.IncRef()
	v.AppendAll(data.Get())

	data.DecRef()

	id.pool, id.data = p, v

	return id
}

// heapRetrieve is a helper that retrieves some bytes and keeps a reference
// to it considering all bytes retrieved from the heap are permanent.
func (p *nativeIdentifierPool) heapRetrieve(v *id, length int) checked.Bytes {
	bytes := p.heap.Get(length)
	key := uintptr(unsafe.Pointer(&v))
	value := unsafe.Pointer(&bytes)
	p.refs.Lock()
	p.refs.idRefsToBytesRefs[key] = value
	p.refs.Unlock()
	return bytes
}

// heapRelease will release any reference so some checked out heap bytes
// the pool might have.
func (p *nativeIdentifierPool) heapRelease(v *id) {
	key := uintptr(unsafe.Pointer(&v))
	p.refs.Lock()
	delete(p.refs.idRefsToBytesRefs, key)
	p.refs.Unlock()
}
