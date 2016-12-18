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

	"github.com/m3db/m3db/context"
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

func (p *simpleIdentifierPool) GetBinaryID(ctx context.Context, v []byte) ID {
	id := p.pool.Get().(*id)
	id.data = v
	ctx.RegisterFinalizer(id)

	return id
}

func (p *simpleIdentifierPool) GetStringID(ctx context.Context, v string) ID {
	return p.GetBinaryID(ctx, []byte(v))
}

func (p *simpleIdentifierPool) Put(v ID) {
	v.Reset(nil)
	p.pool.Put(v)
}

func (p *simpleIdentifierPool) Clone(other ID) ID {
	id := p.pool.Get().(*id)
	id.data = other.Data()

	return id
}

// NewNativeIdentifierPool constructs a new NativeIdentifierPool.
func NewNativeIdentifierPool(
	heap pool.BytesPool, options pool.ObjectPoolOptions) IdentifierPool {

	if options == nil {
		options = pool.NewObjectPoolOptions()
	}

	return &nativeIdentifierPool{
		pool: pool.NewNativePool(pool.NativePoolOptions{
			Type: reflect.TypeOf(id{}),
			Size: uint(options.Size()),
		}), heap: configureHeap(heap)}
}

type nativeIdentifierPool struct {
	pool pool.NativePool
	heap pool.BytesPool
}

func configureHeap(heap pool.BytesPool) pool.BytesPool {
	if heap != nil {
		return heap
	}

	p := pool.NewNativeHeap([]pool.Bucket{
		{Capacity: 128, Count: 4096},
		{Capacity: 256, Count: 2048}}, nil)
	p.Init()

	return p
}

func create() interface{} {
	return &id{}
}

// GetBinaryID returns a new ID based on a binary value.
func (p *nativeIdentifierPool) GetBinaryID(ctx context.Context, v []byte) ID {
	id := p.pool.GetOr(create).(*id)
	id.pool, id.data = p, append(p.heap.Get(len(v)), v...)
	ctx.RegisterFinalizer(id)

	return id
}

// GetStringID returns a new ID based on a string value.
func (p *nativeIdentifierPool) GetStringID(ctx context.Context, v string) ID {
	id := p.pool.GetOr(create).(*id)
	id.pool, id.data = p, append(p.heap.Get(len(v)), v...)
	ctx.RegisterFinalizer(id)

	return id
}

func (p *nativeIdentifierPool) Put(v ID) {
	id := v.(*id)

	p.heap.Put(id.data)
	id.Reset(nil)

	p.pool.Put(id)
}

// Clone replicates given ID into a new ID from the pool.
func (p *nativeIdentifierPool) Clone(v ID) ID {
	id := p.pool.GetOr(create).(*id)
	id.pool, id.data = p, append(p.heap.Get(len(v.Data())), v.Data()...)

	return id
}
