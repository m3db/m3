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

package encoding

import "hash/crc32"

// Checksum is a checksum of a stream of bytes.
type Checksum uint32

// NewChecksum returns a new checksum.
func NewChecksum() *Checksum {
	return new(Checksum)
}

// Get returns the current value of the checksum.
func (c *Checksum) Get() uint32 { return uint32(*c) }

// Reset resets the value of the checksum.
func (c *Checksum) Reset() { *c = 0 }

// Update updates the checksum.
func (c *Checksum) Update(b []byte) {
	curr := c.Get()
	*c = Checksum(crc32.Update(curr, crc32.IEEETable, b))
}
