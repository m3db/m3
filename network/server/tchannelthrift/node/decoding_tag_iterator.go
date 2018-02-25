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
	"bytes"
	"fmt"

	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

var (
	decodingTagIterStateZeroed = decodingTagIterState{}
	emptyBytesSlice            = []byte{}

	_fieldSeperator = []byte("__")
	_valueSeperator = []byte("=")
)

// newDecodingTagIterator returns a DecodingTagIterator over a slice.
func newDecodingTagIterator(idPool ident.Pool, pool decodingTagIterPool) ident.TagIterator {
	iter := &decodingTagIter{
		idPool: idPool,
		pool:   pool,
	}
	return iter
}

type decodingTagIterState struct {
	backingBytes []byte

	currentTagSet bool
	currentTag    ident.Tag
	err           error
}

type decodingTagIter struct {
	decodingTagIterState

	idPool ident.Pool
	pool   decodingTagIterPool
}

func (i *decodingTagIter) Next() bool {
	if i.err != nil {
		return false
	}

	i.release()
	// lazily allocate the Tag when Current is called
	return len(i.backingBytes) > 0
}

func (i *decodingTagIter) release() {
	// release any held resources held by the tag
	if i.currentTagSet {
		i.idPool.PutTag(i.currentTag)
		i.currentTag = ident.Tag{}
	}
	i.currentTagSet = false
}

func (i *decodingTagIter) Current() ident.Tag {
	if i.currentTagSet {
		return i.currentTag
	}

	if len(i.backingBytes) <= 0 {
		// should never happen
		i.err = fmt.Errorf("illegal internal state, len(backingBytes): %d", len(i.backingBytes))
		return ident.Tag{}
	}

	// expected format
	// "tagName1=tagValue1__tagName2=tagValue2..."
	mid := bytes.Index(i.backingBytes, _valueSeperator)
	if mid == -1 {
		// this field has no values, i.e. internal error
		i.err = fmt.Errorf("illegal internal state, no valueSeperator, %v", i.backingBytes)
		return ident.Tag{}
	}

	end := bytes.Index(i.backingBytes, _fieldSeperator)
	if end == -1 {
		// no more fields
		end = len(i.backingBytes)
	}

	name := i.backingBytes[0:mid]
	value := i.backingBytes[mid+len(_fieldSeperator)-1 : end]

	// allocate tag
	// TODO(prateek): this is stupid, change once we have idPool.BinaryID([]byte) -> ID
	i.currentTag.Name = ident.StringID(string(name))
	i.currentTag.Value = ident.StringID(string(value))
	i.currentTagSet = true

	if end == -1 || end+len(_fieldSeperator)-1 >= len(i.backingBytes) {
		i.backingBytes = i.backingBytes[:0]
	} else {
		i.backingBytes = i.backingBytes[end+len(_fieldSeperator):]
	}

	return i.currentTag
}

func (i *decodingTagIter) Err() error {
	return i.err
}

func (i *decodingTagIter) Close() {
	i.release()
	i.decodingTagIterState = decodingTagIterStateZeroed
	if i.pool != nil {
		i.pool.Put(i)
	}
}

func (i *decodingTagIter) Remaining() int {
	if len(i.backingBytes) == 0 {
		return 0
	}
	return bytes.Count(i.backingBytes, _fieldSeperator) + 1
}

func (i *decodingTagIter) Clone() ident.TagIterator {
	iter := i.pool.Get()
	iter.decodingTagIterState = i.decodingTagIterState
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

func (i *decodingTagIter) Reset(b []byte) {
	i.release()
	i.decodingTagIterState = decodingTagIterStateZeroed

	idx := bytes.Index(b, _fieldSeperator)
	if idx == -1 || idx+len(_fieldSeperator)-1 >= len(b) {
		i.backingBytes = emptyBytesSlice
	} else {
		i.backingBytes = b[idx+len(_fieldSeperator):]
	}

}

type decodingTagIterPool interface {
	Init()

	Get() *decodingTagIter

	Put(*decodingTagIter)
}

type decodingIterPool struct {
	idPool ident.Pool
	pool   pool.ObjectPool
}

func newDecodingTagIterPool(
	idPool ident.Pool,
	opts pool.ObjectPoolOptions,
) decodingTagIterPool {
	p := pool.NewObjectPool(opts)
	return &decodingIterPool{
		idPool: idPool,
		pool:   p,
	}
}

func (p *decodingIterPool) Init() {
	p.pool.Init(func() interface{} {
		return newDecodingTagIterator(p.idPool, p)
	})
}

func (p *decodingIterPool) Get() *decodingTagIter {
	return p.pool.Get().(*decodingTagIter)
}

func (p *decodingIterPool) Put(i *decodingTagIter) {
	p.pool.Put(i)
}
