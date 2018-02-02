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

package ident

import (
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/pool"
)

// NewIdentifierPool constructs a new simple IdentifierPool.
func NewIdentifierPool(
	bytesPool pool.CheckedBytesPool,
	options pool.ObjectPoolOptions,
) IdentifierPool {
	p := &simpleIdentifierPool{
		bytesPool: bytesPool,
		pool:      pool.NewObjectPool(options),
	}
	p.pool.Init(func() interface{} { return &id{pool: p} })

	return p
}

type simpleIdentifierPool struct {
	bytesPool pool.CheckedBytesPool
	pool      pool.ObjectPool
}

func (p *simpleIdentifierPool) GetBinaryID(ctx context.Context, v checked.Bytes) ID {
	id := p.pool.Get().(*id)
	ctx.RegisterFinalizer(id)

	v.IncRef()
	id.pool, id.data = p, v

	return id
}

func (p *simpleIdentifierPool) GetStringID(ctx context.Context, v string) ID {
	data := p.bytesPool.Get(len(v))
	data.IncRef()
	data.AppendAll([]byte(v))
	data.DecRef()
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

	newData := p.bytesPool.Get(data.Len())
	newData.IncRef()
	newData.AppendAll(data.Get())

	data.DecRef()

	id.pool, id.data = p, newData

	return id
}

// NewNativeIdentifierPool constructs a new NativeIdentifierPool.
func NewNativeIdentifierPool(
	heap pool.CheckedBytesPool,
	opts pool.ObjectPoolOptions,
) IdentifierPool {
	if opts == nil {
		opts = pool.NewObjectPoolOptions()
	}

	iopts := opts.InstrumentOptions()

	p := &nativeIdentifierPool{
		pool: pool.NewObjectPool(opts.SetInstrumentOptions(
			iopts.SetMetricsScope(iopts.MetricsScope().SubScope("id-pool")))),
		heap: configureHeap(heap),
	}
	p.pool.Init(func() interface{} {
		return &id{pool: p}
	})
	return p
}

type nativeIdentifierPool struct {
	// NB(r): We originally were using a `pool.NativePool`` here for pooling the
	// `id` structs and this worked fine when the `id` structs had no references
	// to anything except longly lived objects.  Now however the `id` structs
	// have references to `checked.Bytes` which need to have GC roots or else
	// they are collected and become invalid references held by the `id` structs.
	// The cheapest way to keep a GC root to them is to simply have `id`
	// structs have a GC root themselves too, hence using the simple object pool
	// here.
	// In the future we could potentially craft a special `checked.Bytes` that
	// has no references to anything itself and can be pooled by the
	// `pool.NativePool` itself too.
	pool pool.ObjectPool
	heap pool.CheckedBytesPool
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

// GetBinaryID returns a new ID based on a binary value.
func (p *nativeIdentifierPool) GetBinaryID(ctx context.Context, v checked.Bytes) ID {
	id := p.pool.Get().(*id)
	ctx.RegisterFinalizer(id)

	v.IncRef()
	id.pool, id.data = p, v

	return id
}

// GetStringID returns a new ID based on a string value.
func (p *nativeIdentifierPool) GetStringID(ctx context.Context, str string) ID {
	id := p.pool.Get().(*id)
	ctx.RegisterFinalizer(id)

	v := p.heap.Get(len(str))
	v.IncRef()
	v.AppendAll([]byte(str))

	id.pool, id.data = p, v

	return id
}

func (p *nativeIdentifierPool) Put(v ID) {
	v.Reset()
	p.pool.Put(v.(*id))
}

// Clone replicates given ID into a new ID from the pool.
func (p *nativeIdentifierPool) Clone(existing ID) ID {
	id := p.pool.Get().(*id)

	data := existing.Data()
	data.IncRef()

	v := p.heap.Get(data.Len())
	v.IncRef()
	v.AppendAll(data.Get())

	data.DecRef()

	id.pool, id.data = p, v

	return id
}
