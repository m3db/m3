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

package pilosa

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/richardartoul/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	b := roaring.NewBitmap(1, 2, 4, 3)
	iter := NewIterator(b.Iterator())
	require.True(t, iter.Next())
	require.Equal(t, postings.ID(1), iter.Current())
	require.True(t, iter.Next())
	require.Equal(t, postings.ID(2), iter.Current())
	require.True(t, iter.Next())
	require.Equal(t, postings.ID(3), iter.Current())
	require.True(t, iter.Next())
	require.Equal(t, postings.ID(4), iter.Current())
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
}
