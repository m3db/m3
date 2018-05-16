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

package digest

import (
	"hash/adler32"

	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/stackadler32"
)

// NewDigest creates a new digest.
// The default 32-bit hashing algorithm is adler32.
func NewDigest() stackadler32.Digest {
	return stackadler32.NewDigest()
}

// SegmentChecksum returns the 32-bit checksum for a segment
// avoiding any allocations.
func SegmentChecksum(segment ts.Segment) uint32 {
	d := stackadler32.NewDigest()
	if segment.Head != nil {
		d = d.Update(segment.Head.Bytes())
	}
	if segment.Tail != nil {
		d = d.Update(segment.Tail.Bytes())
	}
	return d.Sum32()
}

// Checksum returns the checksum for a buffer.
func Checksum(buf []byte) uint32 {
	return adler32.Checksum(buf)
}
