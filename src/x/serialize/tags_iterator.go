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

package xserialize

import (
	"bytes"

	"github.com/m3db/m3/src/dbnode/serialize"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
)

// EncodedTagsIterator iterates over a set of tags.
type EncodedTagsIterator interface {
	id.ID
	id.SortedTagIterator
	NumTags() int
}

type encodedTagsIter struct {
	tagDecoder serialize.TagDecoder
	bytes      checked.Bytes
	pool       *EncodedTagsIteratorPool
}

// NewEncodedTagsIterator creates an EncodedTagsIterator.
func NewEncodedTagsIterator(
	tagDecoder serialize.TagDecoder,
	pool *EncodedTagsIteratorPool,
) EncodedTagsIterator {
	return &encodedTagsIter{
		tagDecoder: tagDecoder,
		bytes:      checked.NewBytes(nil, nil),
		pool:       pool,
	}
}

func (it *encodedTagsIter) Reset(sortedTagPairs []byte) {
	it.bytes.IncRef()
	it.bytes.Reset(sortedTagPairs)
	it.tagDecoder.Reset(it.bytes)
}

func (it *encodedTagsIter) Bytes() []byte {
	return it.bytes.Bytes()
}

func (it *encodedTagsIter) NumTags() int {
	return it.tagDecoder.Len()
}

func (it *encodedTagsIter) TagValue(tagName []byte) ([]byte, bool) {
	iter := it.tagDecoder.Duplicate()
	defer iter.Close()

	for iter.Next() {
		tag := iter.Current()
		if bytes.Equal(tagName, tag.Name.Bytes()) {
			return tag.Value.Bytes(), true
		}
	}
	return nil, false
}

func (it *encodedTagsIter) Next() bool {
	return it.tagDecoder.Next()
}

func (it *encodedTagsIter) Current() ([]byte, []byte) {
	tag := it.tagDecoder.Current()
	return tag.Name.Bytes(), tag.Value.Bytes()
}

func (it *encodedTagsIter) Err() error {
	return it.tagDecoder.Err()
}

func (it *encodedTagsIter) Close() {
	it.bytes.Reset(nil)
	it.bytes.DecRef()
	it.tagDecoder.Reset(it.bytes)

	if it.pool != nil {
		it.pool.Put(it)
	}
}

// EncodedTagsIteratorPool pools EncodedTagsIterator.
type EncodedTagsIteratorPool struct {
	tagDecoderPool serialize.TagDecoderPool
	pool           pool.ObjectPool
}

// NewEncodedTagsIteratorPool creates an EncodedTagsIteratorPool.
func NewEncodedTagsIteratorPool(
	tagDecoderPool serialize.TagDecoderPool,
	opts pool.ObjectPoolOptions,
) *EncodedTagsIteratorPool {
	return &EncodedTagsIteratorPool{
		tagDecoderPool: tagDecoderPool,
		pool:           pool.NewObjectPool(opts),
	}
}

// Init initializes the pool.
func (p *EncodedTagsIteratorPool) Init() {
	p.pool.Init(func() interface{} {
		return NewEncodedTagsIterator(p.tagDecoderPool.Get(), p)
	})
}

// Get provides an EncodedTagsIterator from the pool.
func (p *EncodedTagsIteratorPool) Get() EncodedTagsIterator {
	return p.pool.Get().(*encodedTagsIter)
}

// Put returns an EncodedTagsIterator to the pool.
func (p *EncodedTagsIteratorPool) Put(v EncodedTagsIterator) {
	p.pool.Put(v)
}
