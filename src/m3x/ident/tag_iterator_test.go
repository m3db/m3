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

func testTags() Tags {
	return NewTags(
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
		StringTag("and", "done"),
	)
}

func TestTagStringIteratorInvalid(t *testing.T) {
	require.Panics(t, func() {
		MustNewTagStringsIterator("hello")
	})
	require.Panics(t, func() {
		MustNewTagStringsIterator("hello", "a", "b")
	})
}

func TestTagStringIteratorValid(t *testing.T) {
	iter, err := NewTagStringsIterator("hello", "there")
	require.NoError(t, err)
	require.True(t, iter.Next())
	require.True(t, StringTag("hello", "there").Equal(iter.Current()))
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())
	iter.Close()
}

func TestTagSliceIterator(t *testing.T) {
	expected := map[string]string{
		"foo":   "bar",
		"hello": "there",
		"and":   "done",
	}
	iter := NewTagsIterator(testTags())
	testTagIteratorValues(t, expected, iter)
}

func TestTagSliceIteratorReset(t *testing.T) {
	iter := NewTagsIterator(NewTags(
		StringTag("foo", "bar"),
		StringTag("qux", "qaz"),
	))
	testTagIteratorValues(t, map[string]string{
		"foo": "bar",
		"qux": "qaz",
	}, iter)
	iter.Reset(NewTags(
		StringTag("foo", "bar"),
		StringTag("baz", "qux"),
	))
	testTagIteratorValues(t, map[string]string{
		"foo": "bar",
		"baz": "qux",
	}, iter)
}

func testTagIteratorValues(
	t *testing.T,
	expected map[string]string,
	iter TagIterator,
) {
	require.Equal(t, len(expected), iter.Remaining())
	for iter.Next() {
		c := iter.Current()
		if c.Value.String() == expected[c.Name.String()] {
			delete(expected, c.Name.String())
			continue
		}
		require.Equal(t, len(expected), iter.Remaining())
		require.Fail(t, "unknown tag", c)
	}
	require.Empty(t, expected)
}

func TestTagIterator(t *testing.T) {
	expected := map[string]string{
		"foo":   "bar",
		"hello": "there",
	}
	iter := NewTagsIterator(NewTags(
		StringTag("hello", "there"), StringTag("foo", "bar"),
	))
	require.Equal(t, len(expected), iter.Remaining())
	for iter.Next() {
		c := iter.Current()
		if c.Value.String() == expected[c.Name.String()] {
			delete(expected, c.Name.String())
			continue
		}
		require.Equal(t, len(expected), iter.Remaining())
		require.Fail(t, "unknown tag", c)
	}
	require.Empty(t, expected)
}

func TestTagIteratorDuplicate(t *testing.T) {
	expected := map[string]string{
		"foo":   "bar",
		"hello": "there",
	}
	iter := NewTagsIterator(NewTags(
		StringTag("hello", "there"), StringTag("foo", "bar"),
	))
	clone := iter.Duplicate()
	expectedLen := len(expected)
	require.Equal(t, expectedLen, iter.Remaining())
	for iter.Next() {
		c := iter.Current()
		if c.Value.String() == expected[c.Name.String()] {
			delete(expected, c.Name.String())
			continue
		}
		require.Equal(t, len(expected), iter.Remaining())
		require.Fail(t, "unknown tag", c)
	}
	require.Empty(t, expected)
	require.Equal(t, expectedLen, clone.Remaining())
}

func TestTagIteratorDuplicateFromPool(t *testing.T) {
	expected := map[string]string{
		"foo": "bar",
		"baz": "buz",
	}
	iter := newTestSimplePool().TagsIterator()
	iter.Reset(NewTags(
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
		StringTag("baz", "buz"),
	))
	// First proceed by one
	require.True(t, iter.Next())
	// Now duplicate and test equal
	clone := iter.Duplicate()
	expectedLen := len(expected)
	require.Equal(t, expectedLen, iter.Remaining())
	for iter.Next() {
		c := iter.Current()
		if c.Value.String() == expected[c.Name.String()] {
			delete(expected, c.Name.String())
			continue
		}
		require.Equal(t, len(expected), iter.Remaining())
		require.Fail(t, "unknown tag", c)
	}
	require.Empty(t, expected)
	require.Equal(t, expectedLen, clone.Remaining())
}
