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

package ts

import (
	"github.com/m3db/m3x/pool"
)

const (
	// defaultWritePoolMaxBatchSize is the default maximum size for a writeBatch that the pool
	// will allow to remain in the pool. Any batches larger than that will be discarded to prevent
	// excessive memory use forever in the case of an exceptionally large batch write.
	defaultMaxBatchSize = 100000
)

// WriteBatchPool is a pool of WriteBatch.
type WriteBatchPool struct {
	pool pool.ObjectPool
	// Control how large a pooled batch is allowed to grow before the pool will
	// discard it. This prevents exceptionally large batches from causing high
	// memory usage forever.
	maxBatchSize int
}

// NewWriteBatchPool constructs a new WriteBatchPool.
func NewWriteBatchPool(opts pool.ObjectPoolOptions, maxBatchSize *int) *WriteBatchPool {
	batchSize := defaultMaxBatchSize
	if maxBatchSize != nil {
		batchSize = *maxBatchSize
	}

	p := pool.NewObjectPool(opts)
	return &WriteBatchPool{pool: p, maxBatchSize: batchSize}
}

// Init initializes a WriteBatchPool.
func (p *WriteBatchPool) Init() {
	p.pool.Init(func() interface{} {
		return NewWriteBatch(1000, p.maxBatchSize, nil, p.Put)
	})
}

// Get retrieves a WriteBatch from the pool.
func (p *WriteBatchPool) Get() WriteBatch {
	w := p.pool.Get().(WriteBatch)
	return w
}

// Put stores a WriteBatch in the pool.
func (p *WriteBatchPool) Put(w WriteBatch) {
	p.pool.Put(w)
}
