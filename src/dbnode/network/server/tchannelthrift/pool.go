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
	emptyBlockMetadata   rpc.BlockMetadata
	emptyBlockMetadataV2 rpc.BlockMetadataV2
	emptyBlocksMetadata  rpc.BlocksMetadata
)

// BlockMetadataPool provides a pool for block metadata
type BlockMetadataPool interface {
	// Get returns a block metadata
	Get() *rpc.BlockMetadata

	// Put puts a block metadata back to pool
	Put(m *rpc.BlockMetadata)
}

// BlockMetadataV2Pool provides a pool for block metadata
type BlockMetadataV2Pool interface {
	// Get returns a block metadata
	Get() *rpc.BlockMetadataV2

	// Put puts a block metadata back to pool
	Put(m *rpc.BlockMetadataV2)
}

// BlockMetadataSlicePool provides a pool for block metadata slices
type BlockMetadataSlicePool interface {
	// Get returns a block metadata slice
	Get() []*rpc.BlockMetadata

	// Put puts a block metadata slice back to pool
	Put(m []*rpc.BlockMetadata)
}

// BlockMetadataV2SlicePool provides a pool for block metadata slices
type BlockMetadataV2SlicePool interface {
	// Get returns a block metadata slice
	Get() []*rpc.BlockMetadataV2

	// Put puts a block metadata slice back to pool
	Put(m []*rpc.BlockMetadataV2)
}

// BlocksMetadataPool provides a pool for blocks metadata
type BlocksMetadataPool interface {
	// Get returns a blocks metadata
	Get() *rpc.BlocksMetadata

	// Put puts a blocks metadata back to pool
	Put(m *rpc.BlocksMetadata)
}

// BlocksMetadataSlicePool provides a pool for blocks metadata slices
type BlocksMetadataSlicePool interface {
	// Get returns a blocks metadata slice
	Get() []*rpc.BlocksMetadata

	// Put puts a blocks metadata slice back to pool
	Put(m []*rpc.BlocksMetadata)
}

type blockMetadataPool struct {
	pool pool.ObjectPool
}

// NewBlockMetadataPool creates a new block metadata pool
func NewBlockMetadataPool(opts pool.ObjectPoolOptions) BlockMetadataPool {
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		return rpc.NewBlockMetadata()
	})
	return &blockMetadataPool{pool: p}
}

func (p *blockMetadataPool) Get() *rpc.BlockMetadata {
	return p.pool.Get().(*rpc.BlockMetadata)
}

func (p *blockMetadataPool) Put(metadata *rpc.BlockMetadata) {
	if metadata != nil {
		*metadata = emptyBlockMetadata
		p.pool.Put(metadata)
	}
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

type blockMetadataSlicePool struct {
	pool     pool.ObjectPool
	capacity int
}

// NewBlockMetadataSlicePool creates a new blockMetadataSlice pool
func NewBlockMetadataSlicePool(opts pool.ObjectPoolOptions, capacity int) BlockMetadataSlicePool {
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		return make([]*rpc.BlockMetadata, 0, capacity)
	})
	return &blockMetadataSlicePool{pool: p, capacity: capacity}
}

func (p *blockMetadataSlicePool) Get() []*rpc.BlockMetadata {
	return p.pool.Get().([]*rpc.BlockMetadata)
}

func (p *blockMetadataSlicePool) Put(res []*rpc.BlockMetadata) {
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

type blocksMetadataPool struct {
	pool pool.ObjectPool
}

// NewBlocksMetadataPool creates a new blocks metadata pool
func NewBlocksMetadataPool(opts pool.ObjectPoolOptions) BlocksMetadataPool {
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		return rpc.NewBlocksMetadata()
	})
	return &blocksMetadataPool{pool: p}
}

func (p *blocksMetadataPool) Get() *rpc.BlocksMetadata {
	return p.pool.Get().(*rpc.BlocksMetadata)
}

func (p *blocksMetadataPool) Put(metadata *rpc.BlocksMetadata) {
	if metadata != nil {
		*metadata = emptyBlocksMetadata
		p.pool.Put(metadata)
	}
}

type blocksMetadataSlicePool struct {
	pool     pool.ObjectPool
	capacity int
}

// NewBlocksMetadataSlicePool creates a new blocksMetadataSlice pool
func NewBlocksMetadataSlicePool(opts pool.ObjectPoolOptions, capacity int) BlocksMetadataSlicePool {
	p := pool.NewObjectPool(opts)
	p.Init(func() interface{} {
		return make([]*rpc.BlocksMetadata, 0, capacity)
	})
	return &blocksMetadataSlicePool{pool: p, capacity: capacity}
}

func (p *blocksMetadataSlicePool) Get() []*rpc.BlocksMetadata {
	return p.pool.Get().([]*rpc.BlocksMetadata)
}

func (p *blocksMetadataSlicePool) Put(res []*rpc.BlocksMetadata) {
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
