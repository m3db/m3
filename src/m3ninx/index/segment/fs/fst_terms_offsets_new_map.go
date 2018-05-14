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

package fs

import (
	"bytes"

	"github.com/cespare/xxhash"
)

// newFSTTermsOffsetsMap returns a new fstTermsOffsetsMap with default ctor parameters.
func newFSTTermsOffsetsMap(initialSize int) *fstTermsOffsetsMap {
	return _fstTermsOffsetsMapAlloc(_fstTermsOffsetsMapOptions{
		hash: func(k []byte) fstTermsOffsetsMapHash {
			return fstTermsOffsetsMapHash(xxhash.Sum64(k))
		},
		equals: func(f, g []byte) bool {
			return bytes.Equal(f, g)
		},
		copy:        undefinedFSTTermsOffsetsMapCopyFn,
		finalize:    undefinedFSTTermsOffsetsMapFinalizeFn,
		initialSize: initialSize,
	})
}

var undefinedFSTTermsOffsetsMapCopyFn fstTermsOffsetsMapCopyFn = func([]byte) []byte {
	// NB: intentionally not defined to force users of the map to not
	// allocate extra copies.
	panic("not implemented")
}

var undefinedFSTTermsOffsetsMapFinalizeFn fstTermsOffsetsMapFinalizeFn = func([]byte) {
	// NB: intentionally not defined to force users of the map to not
	// allocate extra copies.
	panic("not implemented")
}
