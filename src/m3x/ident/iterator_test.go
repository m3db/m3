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

func stringIDs() []string {
	return []string{
		"foo",
		"bar",
		"baz",
	}
}

func testIDs() []ID {
	raw := stringIDs()
	ids := make([]ID, 0, len(raw))
	for _, id := range raw {
		ids = append(ids, StringID(id))
	}
	return ids
}

func TestSliceIterator(t *testing.T) {
	expected := map[string]struct{}{
		"foo": struct{}{},
		"bar": struct{}{},
		"baz": struct{}{},
	}
	iter := NewIDSliceIterator(testIDs())
	require.Equal(t, len(expected), iter.Remaining())
	for iter.Next() {
		c := iter.Current()
		if _, ok := expected[c.String()]; ok {
			delete(expected, c.String())
			continue
		}
		require.Equal(t, len(expected), iter.Remaining())
		require.Fail(t, "unknown id", c.String())
	}
	require.Empty(t, expected)
}

func TestStringSliceIterator(t *testing.T) {
	expected := map[string]struct{}{
		"foo": struct{}{},
		"bar": struct{}{},
		"baz": struct{}{},
	}
	iter := NewStringIDsSliceIterator(stringIDs())
	require.Equal(t, len(expected), iter.Remaining())
	for iter.Next() {
		c := iter.Current()
		if _, ok := expected[c.String()]; ok {
			delete(expected, c.String())
			continue
		}
		require.Equal(t, len(expected), iter.Remaining())
		require.Fail(t, "unknown id", c.String())
	}
	require.Empty(t, expected)
}
