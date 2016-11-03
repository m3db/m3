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
	"github.com/m3db/m3db/pool"
)

type identifierPool struct {
	pool pool.NativePool
	heap pool.BytesPool
}

// NewIdentifierPool constructs a new IdentifierPool
func NewIdentifierPool(
	heap pool.BytesPool, options pool.ObjectPoolOptions) IdentifierPool {

	if options == nil {
		options = pool.NewObjectPoolOptions()
	}

	return &identifierPool{
		pool: pool.NewNativePool(pool.NativePoolOptions{
			Type: reflect.TypeOf(id{}),
			Size: uint(options.Size()),
		}), heap: configureHeap(heap)}
}

func configureHeap(heap pool.BytesPool) pool.BytesPool {
	if heap != nil {
		return heap
	}

	return pool.NewNativeHeap([]pool.Bucket{
		{Capacity: 128, Count: 4096},
		{Capacity: 256, Count: 2048}})
}

func create() interface{} {
	return &id{}
}

// GetBinaryID returns a new ID based on a binary value
func (p *identifierPool) GetBinaryID(ctx context.Context, v []byte) ID {
	id := p.pool.GetOr(create).(*id)
	id.pool, id.data = p, append(p.heap.Get(len(v)), v...)
	ctx.RegisterCloser(id)

	return id
}

// GetStringID returns a new ID based on a string value
func (p *identifierPool) GetStringID(ctx context.Context, v string) ID {
	return p.GetBinaryID(ctx, []byte(v))
}

// Clone replicates given ID into a new ID from the pool
func (p *identifierPool) Clone(v ID) ID {
	id := p.pool.GetOr(create).(*id)
	id.pool, id.data = p, append(p.heap.Get(len(v.Data())), v.Data()...)

	return id
}
