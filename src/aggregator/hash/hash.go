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

// Package hash is a temporary measure used as in between while m3aggregator
// is upgraded to use native source generated maps now accessible in m3x,
// these types are missing from m3x when native source generated maps
// were added.
package hash

import murmur3 "github.com/m3db/stackmurmur3/v2"

// Hash128 is a 128-bit hash of an ID consisting of two unsigned 64-bit ints.
type Hash128 struct {
	h0 uint64
	h1 uint64
}

// Murmur3Hash128 computes the 128-bit hash of an id.
func Murmur3Hash128(data []byte) Hash128 {
	h0, h1 := murmur3.Sum128(data)
	return Hash128{h0, h1}
}
