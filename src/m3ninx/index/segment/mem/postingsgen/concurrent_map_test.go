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

package postingsgen

import (
	"regexp"
	"testing"

	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/postings/roaring"

	"github.com/stretchr/testify/require"
)

func TestConcurrentMap(t *testing.T) {
	opts := ConcurrentMapOpts{
		InitialSize:      1024,
		PostingsListPool: postings.NewPool(nil, roaring.NewPostingsList),
	}
	pm := NewConcurrentMap(opts)

	require.NoError(t, pm.Add([]byte("foo"), 1))
	require.NoError(t, pm.Add([]byte("bar"), 2))
	require.NoError(t, pm.Add([]byte("foo"), 3))
	require.NoError(t, pm.Add([]byte("baz"), 4))

	pl := pm.Get([]byte("foo"))
	require.Equal(t, 2, pl.Len())
	require.True(t, pl.Contains(1))
	require.True(t, pl.Contains(3))

	// No matches.
	pl = pm.Get([]byte("fizz"))
	require.Equal(t, 0, pl.Len())

	re := regexp.MustCompile("ba.*")
	pls := pm.GetRegex(re)
	require.Equal(t, 2, len(pls))

	clone := pls[0].Clone()
	clone.Union(pls[1])
	require.True(t, clone.Contains(2))
	require.True(t, clone.Contains(4))
}
