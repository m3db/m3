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

package tchannelthrift

import (
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3x/pool"
)

var (
	emptyBlockMetadataV2 rpc.BlockMetadataV2
)

// BlockMetadataV2Pool provides a pool for block metadata
type BlockMetadataV2Pool interface {
	// Get returns a block metadata
	Get() *rpc.BlockMetadataV2

	// Put puts a block metadata back to pool
	Put(m *rpc.BlockMetadataV2)
}

// BlockMetadataV2SlicePool provides a pool for block metadata slices
type BlockMetadataV2SlicePool interface {
	// Get returns a block metadata slice
	Get() []*rpc.BlockMetadataV2

	// Put puts a block metadata slice back to pool
	Put(m []*rpc.BlockMetadataV2)
}

type blockMetadataV2Pool struct {
	pool pool.ObjectPool
}

// NewBlockMetadataV2Pool creates a new block metadata pool
func NewBlockMetadataV2Pool(opts pool.ObjectPoolOptions) BlockMetadataV2Pool {
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		return rpc.NewBlockMetadataV2()
	})
	return &blockMetadataV2Pool{pool: p}
}

func (p *blockMetadataV2Pool) Get() *rpc.BlockMetadataV2 {
	return p.pool.Get().(*rpc.BlockMetadataV2)
}

func (p *blockMetadataV2Pool) Put(metadata *rpc.BlockMetadataV2) {
	if metadata != nil {
		*metadata = emptyBlockMetadataV2
		p.pool.Put(metadata)
	}
}

type blockMetadataV2SlicePool struct {
	pool     pool.ObjectPool
	capacity int
}

// NewBlockMetadataV2SlicePool creates a new blockMetadataV2Slice pool
func NewBlockMetadataV2SlicePool(opts pool.ObjectPoolOptions, capacity int) BlockMetadataV2SlicePool {
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		return make([]*rpc.BlockMetadataV2, 0, capacity)
	})
	return &blockMetadataV2SlicePool{pool: p, capacity: capacity}
}

func (p *blockMetadataV2SlicePool) Get() []*rpc.BlockMetadataV2 {
	return p.pool.Get().([]*rpc.BlockMetadataV2)
}

func (p *blockMetadataV2SlicePool) Put(res []*rpc.BlockMetadataV2) {
	if res == nil || cap(res) > p.capacity {
		// Don't return nil or large slices back to pool
		return
	}
	for i := range res {
		res[i] = nil
	}
	res = res[:0]
	p.pool.Put(res)
}
