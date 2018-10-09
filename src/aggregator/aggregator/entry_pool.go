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

package aggregator

import "github.com/m3db/m3x/pool"

// EntryAlloc allocates a new entry.
type EntryAlloc func() *Entry

// EntryPool provides a pool of entries.
type EntryPool interface {
	// Init initializes the entry pool.
	Init(alloc EntryAlloc)

	// Get gets a entry from the pool.
	Get() *Entry

	// Put returns a entry to the pool.
	Put(value *Entry)
}

type entryPool struct {
	pool pool.ObjectPool
}

// NewEntryPool creates a new pool for entries.
func NewEntryPool(opts pool.ObjectPoolOptions) EntryPool {
	return &entryPool{pool: pool.NewObjectPool(opts)}
}

func (p *entryPool) Init(alloc EntryAlloc) {
	p.pool.Init(func() interface{} {
		return alloc()
	})
}

func (p *entryPool) Get() *Entry {
	return p.pool.Get().(*Entry)
}

func (p *entryPool) Put(value *Entry) {
	p.pool.Put(value)
}
