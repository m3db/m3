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

package mem

import (
	"testing"

	"github.com/m3db/fast-skiplist"

	"github.com/stretchr/testify/require"
)

func TestSkipListIteratorSortedOrder(t *testing.T) {
	input := [][]byte{
		[]byte("def"),
		[]byte("abc"),
		[]byte("ghi"),
	}

	list := skiplist.New()
	for _, elem := range input {
		list.Set(elem, nil)
	}

	iter := newSkipListIter(list)
	require.True(t, iter.Next())
	require.Equal(t, []byte("abc"), iter.Current())
	require.True(t, iter.Next())
	require.Equal(t, []byte("def"), iter.Current())
	require.True(t, iter.Next())
	require.Equal(t, []byte("ghi"), iter.Current())
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}
