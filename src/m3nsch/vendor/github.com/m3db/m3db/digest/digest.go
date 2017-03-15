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
	"hash"
	"hash/adler32"

	"github.com/m3db/m3db/ts"
)

// NewDigest creates a new digest.
// The default 32-bit hashing algorithm is adler32.
func NewDigest() hash.Hash32 {
	return adler32.New()
}

// Checksum returns the 32-bit data checksum.
func Checksum(data []byte) uint32 {
	return adler32.Checksum(data)
}

// SegmentChecksum returns the 32-bit checksum for a segment.
func SegmentChecksum(segment ts.Segment) uint32 {
	d := NewDigest()
	if head := segment.Head; head != nil {
		d.Write(head.Get())
	}
	if tail := segment.Tail; tail != nil {
		d.Write(tail.Get())
	}
	return d.Sum32()
}
