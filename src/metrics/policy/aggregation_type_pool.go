// Copyright (c) 2017 Uber Technologies, Inc.
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

package policy

import "github.com/m3db/m3x/pool"

// AggregationTypesAlloc allocates new aggregation types.
type AggregationTypesAlloc func() AggregationTypes

// AggregationTypesPool provides a pool of aggregation types.
type AggregationTypesPool interface {
	// Init initializes the aggregation types pool.
	Init(alloc AggregationTypesAlloc)

	// Get gets an empty list of aggregation types from the pool.
	Get() AggregationTypes

	// Put returns aggregation types to the pool.
	Put(value AggregationTypes)
}

type aggTypesPool struct {
	pool pool.ObjectPool
}

// NewAggregationTypesPool creates a new pool for aggregation types.
func NewAggregationTypesPool(opts pool.ObjectPoolOptions) AggregationTypesPool {
	return &aggTypesPool{pool: pool.NewObjectPool(opts)}
}

func (p *aggTypesPool) Init(alloc AggregationTypesAlloc) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *aggTypesPool) Get() AggregationTypes {
	return p.pool.Get().(AggregationTypes)
}

func (p *aggTypesPool) Put(value AggregationTypes) {
	p.pool.Put(value[:0])
}
