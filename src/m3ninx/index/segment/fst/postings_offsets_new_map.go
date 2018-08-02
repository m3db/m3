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

package fst

import (
	"bytes"

	"github.com/m3db/m3/src/m3ninx/doc"

	"github.com/cespare/xxhash"
)

// newPostingsOffsetsMap returns a new postingsOffsetsMap with default ctor parameters.
func newPostingsOffsetsMap(initialSize int) *postingsOffsetsMap {
	return _postingsOffsetsMapAlloc(_postingsOffsetsMapOptions{
		hash: func(f doc.Field) postingsOffsetsMapHash {
			// NB(prateek): Similar to the standard composite key hashes for Java objects
			hash := uint64(7)
			hash = 31*hash + xxhash.Sum64(f.Name)
			hash = 31*hash + xxhash.Sum64(f.Value)
			return postingsOffsetsMapHash(hash)
		},
		equals: func(f, g doc.Field) bool {
			return bytes.Equal(f.Name, g.Name) && bytes.Equal(f.Value, g.Value)
		},
		copy:        undefinedPostingsOffsetsMapCopyFn,
		finalize:    undefinedPostingsOffsetsMapFinalizeFn,
		initialSize: initialSize,
	})
}

var undefinedPostingsOffsetsMapCopyFn postingsOffsetsMapCopyFn = func(doc.Field) doc.Field {
	// NB: intentionally not defined to force users of the map to not
	// allocate extra copies.
	panic("not implemented")
}

var undefinedPostingsOffsetsMapFinalizeFn postingsOffsetsMapFinalizeFn = func(doc.Field) {
	// NB: intentionally not defined to force users of the map to not
	// allocate extra copies.
	panic("not implemented")
}
