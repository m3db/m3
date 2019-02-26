// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/idx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLRU(t *testing.T) {
	lru, err := NewQueryConversionLRU(5)
	require.NoError(t, err)

	var (
		ok      bool
		evicted bool
		q       idx.Query
	)

	// test add and get
	evicted = lru.Set([]byte("a"), idx.NewTermQuery([]byte("foo"), []byte("bar")))
	require.False(t, evicted)
	evicted = lru.Set([]byte("b"), idx.NewTermQuery([]byte("biz"), []byte("baz")))
	require.False(t, evicted)

	q, ok = lru.Get([]byte("a"))
	require.True(t, ok)
	assert.Equal(t, "term(foo, bar)", q.String())
	q, ok = lru.Get([]byte("b"))
	require.True(t, ok)
	assert.Equal(t, "term(biz, baz)", q.String())

	// fill up the cache
	evicted = lru.Set([]byte("c"), idx.NewTermQuery([]byte("bar"), []byte("foo")))
	require.False(t, evicted)
	evicted = lru.Set([]byte("d"), idx.NewTermQuery([]byte("baz"), []byte("biz")))
	require.False(t, evicted)
	evicted = lru.Set([]byte("e"), idx.NewTermQuery([]byte("qux"), []byte("quz")))
	require.False(t, evicted)
	evicted = lru.Set([]byte("f"), idx.NewTermQuery([]byte("quz"), []byte("qux")))
	require.True(t, evicted)

	// make sure "a" is no longer in the cache
	_, ok = lru.Get([]byte("a"))
	require.False(t, ok)

	// make sure "b" is still in the cache
	q, ok = lru.Get([]byte("b"))
	require.True(t, ok)
	assert.Equal(t, "term(biz, baz)", q.String())

	// rewrite "e" and make sure nothing gets evicted
	// since "e" is already in the cache.
	evicted = lru.Set([]byte("e"), idx.NewTermQuery([]byte("qux"), []byte("quz")))
	require.False(t, evicted)

	// make sure "e" is still in the cache
	q, ok = lru.Get([]byte("e"))
	require.True(t, ok)
	assert.Equal(t, "term(qux, quz)", q.String())
}
