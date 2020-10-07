// Copyright (c) 2020 Uber Technologies, Inc.
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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func b(s string) []byte { return []byte(s) }

func TestIndexEntryHash(t *testing.T) {
	// NB: expected values are verified independently on an online Adler hasher.
	tests := []struct {
		entry    IndexEntry
		expected int64
	}{
		{
			entry:    IndexEntry{ID: b("foo"), EncodedTags: b("bar")},
			expected: 145425018,
		},
		{
			entry:    IndexEntry{ID: b("foo"), EncodedTags: b("bar"), DataChecksum: 8},
			expected: 145425010,
		},
		{
			entry:    IndexEntry{ID: b("foo"), EncodedTags: b("baz")},
			expected: 145949314,
		},
		{
			entry:    IndexEntry{ID: b("zoo"), EncodedTags: b("bar")},
			expected: 153289358,
		},
	}

	hasher := NewAdlerHasher()
	for _, tt := range tests {
		assert.Equal(t, tt.expected, hasher.HashIndexEntry(tt.entry))
	}
}
