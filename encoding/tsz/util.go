// Copyright (c) 2016 Uber Technologies, Inc.
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

package tsz

func leadingAndTrailingZeros(v uint64) (int, int) {
	if v == 0 {
		return 64, 0
	}

	numTrailing := 0
	for tmp := v; (tmp & 1) == 0; tmp >>= 1 {
		numTrailing++
	}

	numLeading := 0
	for tmp := v; (tmp & (1 << 63)) == 0; tmp <<= 1 {
		numLeading++
	}

	return numLeading, numTrailing
}

// signExtend sign extends the highest bit of v which has numBits (<=64)
func signExtend(v uint64, numBits int) int64 {
	shift := uint(64 - numBits)
	return (int64(v) << shift) >> shift
}
