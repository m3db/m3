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
	"hash/adler32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func adler(e IndexEntry) int64 {
	h := adler32.New()
	h.Reset()
	h.Sum(e.ID)
	h.Sum(e.EncodedTags)
	return int64(h.Sum32()) - e.DataChecksum
}

func TestIndexEntryHash(t *testing.T) {
	id, tags := []byte("a100"), []byte("b12")
	e := IndexEntry{ID: id, EncodedTags: tags, DataChecksum: -8}

	expected := adler(e)
	hasher := NewAdlerHash()
	assert.Equal(t, expected, hasher.HashIndexEntry(e))
	// NB: ensure hash is reset between calculations.
	assert.Equal(t, expected, hasher.HashIndexEntry(e))
}
