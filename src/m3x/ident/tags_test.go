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

package ident

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTagsUnequalLength(t *testing.T) {
	tagsA := Tags{
		StringTag("hello", "there"),
	}
	tagsB := Tags{
		StringTag("foo", "bar"),
		StringTag("and", "done"),
	}

	require.False(t, tagsA.Equal(tagsB))
	require.False(t, tagsB.Equal(tagsA))
}

func TestTagsUnequalOrder(t *testing.T) {
	tagsA := Tags{
		StringTag("foo", "bar"),
		StringTag("hello", "there"),
	}
	tagsB := Tags{
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
	}

	require.False(t, tagsA.Equal(tagsB))
	require.False(t, tagsB.Equal(tagsA))
}

func TestTagsEqual(t *testing.T) {
	tagsA := Tags{
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
	}
	tagsB := Tags{
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
	}

	require.True(t, tagsA.Equal(tagsB))
	require.True(t, tagsB.Equal(tagsA))
}
