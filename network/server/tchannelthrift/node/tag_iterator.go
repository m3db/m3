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

package node

import (
	"fmt"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

var (
	tagIterStateZeroed = tagIterState{currentIdx: -1}
)

// newTagIterator returns a TagIterator over a slice.
func newTagIterator(idPool ident.Pool, pool tagIterPool) ident.TagIterator {
	iter := &tagIter{
		idPool: idPool,
		pool:   pool,
	}
	return iter
}

type tagIterState struct {
	backingSlice []*rpc.TagRaw

	currentTagSet bool
	currentTag    ident.Tag
	currentIdx    int
	err           error
}

type tagIter struct {
	tagIterState

	idPool ident.Pool
	pool   tagIterPool
}

func (i *tagIter) Next() bool {
	if i.err != nil {
		return false
	}

	i.release()
	i.currentIdx++
	if i.currentIdx < len(i.backingSlice) {
		// lazily allocate the Tag when Current is called
		return true
	}
	return false
}

func (i *tagIter) release() {
	// release any held resources held by the tag
	if i.currentTagSet {
		i.idPool.PutTag(i.currentTag)
		i.currentTag = ident.Tag{}
	}
	i.currentTagSet = false
}

func (i *tagIter) Current() ident.Tag {
	if i.currentTagSet {
		return i.currentTag
	}

	if i.currentIdx >= len(i.backingSlice) {
		// should never happen
		i.err = fmt.Errorf("illegal internal state, currentIdx: %d, len(backingSlice): %d",
			i.currentIdx, len(i.backingSlice))
		return ident.Tag{}
	}

	// allocate tag
	// TODO(prateek): this is stupid, change once we have idPool.BinaryID([]byte) -> ID
	i.currentTag.Name = ident.StringID(string(i.backingSlice[i.currentIdx].Name))
	i.currentTag.Value = ident.StringID(string(i.backingSlice[i.currentIdx].Value))
	i.currentTagSet = true

	return i.currentTag
}

func (i *tagIter) Err() error {
	return i.err
}

func (i *tagIter) Close() {
	i.release()
	i.tagIterState = tagIterStateZeroed
	if i.pool != nil {
		i.pool.Put(i)
	}
}

func (i *tagIter) Remaining() int {
	if r := len(i.backingSlice) - 1 - i.currentIdx; r >= 0 {
		return r
	}
	return 0
}

func (i *tagIter) Clone() ident.TagIterator {
	iter := i.pool.Get()
	iter.tagIterState = i.tagIterState
	// if we have a current tag, the next iter needs to have an
	// indepenent copy, so as to maintain it's lifecycle
	if i.currentTagSet {
		// TODO(prateek): use idPool.CloneTag
		iter.currentTag = ident.Tag{
			Name:  i.idPool.Clone(i.currentTag.Name),
			Value: i.idPool.Clone(i.currentTag.Value),
		}
	}
	return iter
}

func (i *tagIter) Reset(tags []*rpc.TagRaw) {
	i.release()
	i.tagIterState = tagIterStateZeroed
	i.backingSlice = tags
}

type tagIterPool interface {
	Init()

	Get() *tagIter

	Put(*tagIter)
}

type iterPool struct {
	idPool ident.Pool
	pool   pool.ObjectPool
}

func newTagIterPool(
	idPool ident.Pool,
	opts pool.ObjectPoolOptions,
) tagIterPool {
	p := pool.NewObjectPool(opts)
	return &iterPool{
		idPool: idPool,
		pool:   p,
	}
}

func (p *iterPool) Init() {
	p.pool.Init(func() interface{} {
		return newTagIterator(p.idPool, p)
	})
}

func (p *iterPool) Get() *tagIter {
	return p.pool.Get().(*tagIter)
}

func (p *iterPool) Put(i *tagIter) {
	p.pool.Put(i)
}
