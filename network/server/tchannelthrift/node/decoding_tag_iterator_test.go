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
	"testing"

	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
)

func newDecodingTestTagIter() *decodingTagIter {
	rawTags := []byte("dontmatter__foo=bar__baz=barf")

	idPool := ident.NewPool(nil, nil)
	pool := newDecodingTagIterPool(idPool, nil)
	pool.Init()
	iter := pool.Get()

	iter.Reset(rawTags)
	return iter
}

func TestDecodingTagIter(t *testing.T) {
	iter := newDecodingTestTagIter()
	require.Equal(t, 2, iter.Remaining())

	require.True(t, iter.Next())
	tag := iter.Current()
	require.Equal(t, "foo", tag.Name.String())
	require.Equal(t, "bar", tag.Value.String())

	require.True(t, iter.Next())
	tag = iter.Current()
	require.Equal(t, "baz", tag.Name.String())
	require.Equal(t, "barf", tag.Value.String())

	require.False(t, iter.Next())
	require.Nil(t, iter.Err())
}

func TestDecodingTagIterClone(t *testing.T) {
	iter := newDecodingTestTagIter()
	clone := iter.Clone()
	require.Equal(t, 2, iter.Remaining())
	require.Equal(t, 2, clone.Remaining())

	require.True(t, iter.Next())
	require.Equal(t, 2, clone.Remaining())
}
