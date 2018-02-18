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

package client

import (
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

// DefaultTagArrayPoolSize ...
const DefaultTagArrayPoolSize = 65536

// DefaultTagArrayCapacity ...
const DefaultTagArrayCapacity = 8

// TagArrayPool ...
type TagArrayPool interface {
	// Init pool
	Init()

	// Get an array of ident.Tag objects
	Get() []ident.Tag

	// Put an array of ident.Tag objects
	Put([]ident.Tag)
}

type poolOfIdentTags struct {
	pool     pool.ObjectPool
	capacity int
}

// TODO(prateek): migrate to m3x/ident
// NewTagArrayPool ...
func NewTagArrayPool(
	opts pool.ObjectPoolOptions, capacity int) TagArrayPool {

	p := pool.NewObjectPool(opts)
	return &poolOfIdentTags{p, capacity}
}

func (p *poolOfIdentTags) Init() {
	p.pool.Init(func() interface{} {
		return make([]ident.Tag, 0, p.capacity)
	})
}

func (p *poolOfIdentTags) Get() []ident.Tag {
	return p.pool.Get().([]ident.Tag)
}

func (p *poolOfIdentTags) Put(tags []ident.Tag) {
	for _, t := range tags {
		t.Name.Finalize()
		t.Value.Finalize()
	}
	tags = tags[:0]
	p.pool.Put(tags)
}
