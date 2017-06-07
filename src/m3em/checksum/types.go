// Copyright (c) 2017 Uber Technologies, Inc.
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

package checksum

import (
	"hash/crc32"
)

// Fn returns a checksum of the provided bytes
func Fn(b []byte) uint32 {
	return crc32.ChecksumIEEE(b)
}

// Accumulator accrues checksums
type Accumulator interface {
	// Current returns the current checksum value
	Current() uint32

	// Update updates accrued checksum using provided bytes
	Update(b []byte) uint32
}

// NewAccumulator returns a new accumulator
func NewAccumulator() Accumulator {
	return &accum{
		checksum: 0,
	}
}

type accum struct {
	checksum uint32
}

func (a *accum) Current() uint32 {
	return a.checksum
}

func (a *accum) Update(b []byte) uint32 {
	a.checksum = crc32.Update(a.checksum, crc32.IEEETable, b)
	return a.checksum
}
