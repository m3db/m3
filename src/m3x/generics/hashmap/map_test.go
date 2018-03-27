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

package hashmap

import (
	"testing"

	"github.com/cespare/xxhash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStringMap returns a new generic (non-specialized) version of
// the map that is intended to be used with strings, this is useful for testing
// the non-generated generic map source code.
func newTestStringMap(size int) *Map {
	return newMap(mapOptions{
		hash: func(key KeyType) MapHash {
			return MapHash(xxhash.Sum64([]byte(key.(string))))
		},
		equals: func(x, y KeyType) bool {
			return x.(string) == y.(string)
		},
		copy: func(k KeyType) KeyType {
			// Strings are immutable, so we can just return the same string
			return k
		},
		finalize: func(k KeyType) {
			// No-op, not pooling
		},
		initialSize: size,
	})
}

func TestMapGet(t *testing.T) {
	m := newTestStringMap(0)

	_, ok := m.Get("foo")
	require.False(t, ok)

	m.Set("foo", "bar")

	v, ok := m.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", v.(string))
}

func TestMapSetOverwrite(t *testing.T) {
	m := newTestStringMap(0)

	_, ok := m.Get("foo")
	require.False(t, ok)

	m.Set("foo", "bar")
	m.Set("foo", "baz")
	m.Set("foo", "qux")

	v, ok := m.Get("foo")
	require.True(t, ok)
	require.Equal(t, "qux", v.(string))
}

func TestMapContains(t *testing.T) {
	m := newTestStringMap(0)

	exists := m.Contains("foo")
	require.False(t, exists)

	m.Set("foo", "bar")

	exists = m.Contains("foo")
	require.True(t, exists)
}

func TestMapDelete(t *testing.T) {
	m := newTestStringMap(0)

	m.Set("foo", "a")
	m.Set("bar", "b")
	m.Set("baz", "c")
	require.Equal(t, 3, m.Len())

	m.Delete("foo")
	require.Equal(t, 2, m.Len())

	m.Delete("keyThatDoesNotExist")
	require.Equal(t, 2, m.Len())
}

func TestMapReset(t *testing.T) {
	m := newTestStringMap(0)

	m.Set("foo", "a")
	m.Set("bar", "b")
	m.Set("baz", "c")
	require.Equal(t, 3, m.Len())

	ref := m.lookup
	m.Reset()
	require.Equal(t, 0, m.Len())
	assert.Equal(t, ref, m.lookup)
}

func TestMapReallocate(t *testing.T) {
	m := newTestStringMap(0)

	m.Set("foo", "a")
	m.Set("bar", "b")
	m.Set("baz", "c")
	require.Equal(t, 3, m.Len())

	ref := m.lookup
	m.Reallocate()
	require.Equal(t, 0, m.Len())
	assert.NotEqual(t, ref, m.lookup)
}

func TestMapIter(t *testing.T) {
	m := newTestStringMap(0)

	strMap := make(map[string]string)
	set := func(k, v string) {
		m.Set(k, v)
		strMap[k] = v
	}

	set("foo", "a")
	set("bar", "b")
	set("baz", "c")

	iterated := 0
	for _, entry := range m.Iter() {
		iterated++
		require.Equal(t, strMap[entry.Key().(string)], entry.Value().(string))
	}
	require.Equal(t, len(strMap), iterated)
}

func TestMapCollision(t *testing.T) {
	m := newTestStringMap(0)
	// Always collide
	m.hash = func(_ KeyType) MapHash { return 0 }

	// Insert foo, ensure set at fake hash
	m.Set("foo", "a")

	entry, ok := m.lookup[0]
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key().(string))
	assert.Equal(t, "a", entry.value.(string))

	// Insert bar, ensure collides and both next to each other
	m.Set("bar", "b")

	entry, ok = m.lookup[0]
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key().(string))
	assert.Equal(t, "a", entry.value.(string))

	entry, ok = m.lookup[1]
	assert.True(t, ok)
	assert.Equal(t, "bar", entry.Key().(string))
	assert.Equal(t, "b", entry.value.(string))

	// Test getting both keys works well too
	v, ok := m.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "a", v.(string))

	v, ok = m.Get("bar")
	assert.True(t, ok)
	assert.Equal(t, "b", v.(string))

	// Ensure set for the colliding key works
	m.Set("bar", "c")

	entry, ok = m.lookup[0]
	assert.True(t, ok)
	assert.Equal(t, "foo", entry.Key().(string))
	assert.Equal(t, "a", entry.value.(string))

	entry, ok = m.lookup[1]
	assert.True(t, ok)
	assert.Equal(t, "bar", entry.Key().(string))
	assert.Equal(t, "c", entry.value.(string))

	// Test getting both keys works well too
	v, ok = m.Get("foo")
	assert.True(t, ok)
	assert.Equal(t, "a", v.(string))

	v, ok = m.Get("bar")
	assert.True(t, ok)
	assert.Equal(t, "c", v.(string))
}

func TestMapWithSize(t *testing.T) {
	m := newTestStringMap(42)
	m.Set("foo", "bar")
	v, ok := m.Get("foo")
	require.True(t, ok)
	require.Equal(t, "bar", v.(string))
}

func TestMapSetUnsafeNoCopyKey(t *testing.T) {
	m := newTestStringMap(0)

	copies := 0
	m.copy = func(k KeyType) KeyType {
		copies++
		return k
	}

	m.Set("foo", "a")
	m.Set("bar", "b")
	assert.Equal(t, 2, copies)

	m.SetUnsafe("baz", "c", SetUnsafeOptions{NoCopyKey: true})
	assert.Equal(t, 2, copies)
}

func TestMapSetUnsafeNoCopyNoFinalizeKey(t *testing.T) {
	m := newTestStringMap(0)

	copies := 0
	m.copy = func(k KeyType) KeyType {
		copies++
		return k
	}

	finalizes := 0
	m.finalize = func(k KeyType) {
		finalizes++
	}

	m.Set("foo", "a")
	m.Set("bar", "b")
	assert.Equal(t, 2, copies)

	m.Delete("foo")
	m.Delete("bar")
	assert.Equal(t, 2, finalizes)

	m.SetUnsafe("baz", "c", SetUnsafeOptions{NoCopyKey: true, NoFinalizeKey: true})
	assert.Equal(t, 2, copies)
	assert.Equal(t, 2, finalizes)
}
