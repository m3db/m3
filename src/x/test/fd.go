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

package xtest

import (
	"math"
	"math/rand"

	"github.com/m3db/m3/src/x/os"
)

// CorruptingFD implements the FD interface and can corrupt all writes issued
// to it based on a configurable probability.
type CorruptingFD struct {
	fd                    xos.File
	corruptionProbability float64
}

// NewCorruptingFD creates a new corrupting FD.
func NewCorruptingFD(
	fd xos.File,
	corruptionProbability float64,
	seed int64,
) *CorruptingFD {
	return &CorruptingFD{
		fd: fd,
		corruptionProbability: corruptionProbability,
	}
}

// Write to the underlying f.d with a chance of corrupting it.
func (c *CorruptingFD) Write(p []byte) (int, error) {
	threshold := uint64(c.corruptionProbability * float64(math.MaxUint64))
	if rand.Uint64() <= threshold {
		var (
			byteStart  int
			byteOffset int
		)
		if len(p) > 1 {
			byteStart = rand.Intn(len(p) - 1)
			byteOffset = rand.Intn(len(p) - 1 - byteStart)
		}

		if byteStart >= 0 && byteStart+byteOffset < len(p) {
			copy(p[byteStart:byteStart+byteOffset], make([]byte, byteOffset))
		}
	}
	return c.fd.Write(p)
}

// Sync fsyncs the underlying f.d.
func (c *CorruptingFD) Sync() error {
	return c.fd.Sync()
}

// Close the underlying f.d.
func (c *CorruptingFD) Close() error {
	return c.fd.Close()
}
