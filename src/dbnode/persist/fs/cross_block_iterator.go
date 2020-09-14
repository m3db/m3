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

package fs

import (
	"bytes"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

type crossBlockIterator struct {
	idx        int
	exhausted  bool
	current    encoding.ReaderIterator
	byteReader *bytes.Reader
	records    []BlockRecord
}

// NewCrossBlockIterator creates a new CrossBlockIterator.
func NewCrossBlockIterator(pool encoding.ReaderIteratorPool) CrossBlockIterator {
	return &crossBlockIterator{
		idx:        -1,
		current:    pool.Get(),
		byteReader: bytes.NewReader(nil),
	}
}

func (c *crossBlockIterator) Next() bool {
	if c.exhausted {
		return false
	}

	// NB: if no values remain in current iterator,
	if c.idx < 0 || !c.current.Next() {
		// NB: clear previous.
		if c.idx >= 0 {
			if c.current.Err() != nil {
				c.exhausted = true
				return false
			}
		}

		c.idx++
		if c.idx >= len(c.records) {
			c.exhausted = true
			return false
		}

		c.byteReader.Reset(c.records[c.idx].Data)
		c.current.Reset(c.byteReader, nil)
		// NB: rerun using the next record.
		return c.Next()
	}

	return true
}

func (c *crossBlockIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return c.current.Current()
}

func (c *crossBlockIterator) Reset(records []BlockRecord) {
	c.idx = -1
	c.records = records
	c.exhausted = false
	c.byteReader.Reset(nil)
}

func (c *crossBlockIterator) Close() {
	c.Reset(nil)
	c.current.Close()
}

func (c *crossBlockIterator) Err() error {
	return c.current.Err()
}
