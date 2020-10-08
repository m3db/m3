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

package hash

import (
	"hash"
	"regexp"
	"strconv"
	"testing"

	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/tj/assert"

	"github.com/stretchr/testify/require"
)

type testHash struct {
	t  *testing.T
	re *regexp.Regexp

	v     uint32
	count int
}

func (h *testHash) Reset() {
	h.v = 0
	h.count = 0
}

func (h *testHash) Size() int      { return h.count }
func (h *testHash) BlockSize() int { return 0 }
func (h *testHash) Sum32() uint32  { return h.v }
func (h *testHash) Write(p []byte) (n int, err error) {
	h.Sum(p)
	return 0, nil
}

func (h *testHash) Sum(b []byte) []byte {
	h.count = h.count + 1
	matched := h.re.FindAllString(string(b), -1)
	if len(matched) == 0 {
		return b
	}

	i, err := strconv.Atoi(matched[0])
	require.NoError(h.t, err)
	h.v = h.v + uint32(i)
	return b
}

// NewParseValueHash32 builds a new test hash.Hash32. Given hash value is a sum
// of all integer values from string representations of input bytes.
// e.g. testHash.Sum([]byte("foo123")) will increment hash value by 123.
func NewParseValueHash32(t *testing.T) hash.Hash32 {
	re, err := regexp.Compile(`\d[\d,]*[\.]?[\d{2}]*`)
	require.NoError(t, err)
	return &testHash{t: t, re: re}
}

type parsedIndexHasher struct {
	t  *testing.T
	re *regexp.Regexp
}

// NewParsedIndexHasher builds a new test IndexEntryHasher, hashing IndexEntries
// to the parsed value of their IDs.
func NewParsedIndexHasher(t *testing.T) schema.IndexEntryHasher {
	re, err := regexp.Compile(`\d[\d,]*[\.]?[\d{2}]*`)
	require.NoError(t, err)

	return &parsedIndexHasher{t: t, re: re}
}

func (h *parsedIndexHasher) HashIndexEntry(e schema.IndexEntry) int64 {
	matched := h.re.FindAllString(string(e.ID), -1)
	if len(matched) == 0 {
		return 0
	}

	i, err := strconv.Atoi(matched[0])
	assert.NoError(h.t, err)
	return int64(i)
}
