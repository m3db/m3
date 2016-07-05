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

package encoding

import (
	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"
)

type activeIterState int

const (
	activeIterStateNone activeIterState = iota
	activeIterStateSingle
	activeIterStateMulti
)

type mixedReadersIterator struct {
	singleIter m3db.SingleReaderIterator
	multiIter  m3db.MultiReaderIterator
	slicesIter m3db.ReaderSliceOfSlicesIterator
	state      activeIterState
	err        error
	closed     bool
}

// NewMixedReadersIterator creates a new mixed readers iterator
func NewMixedReadersIterator(
	singleIter m3db.SingleReaderIterator,
	multiIter m3db.MultiReaderIterator,
	slicesIter m3db.ReaderSliceOfSlicesIterator,
) m3db.MixedReadersIterator {
	it := &mixedReadersIterator{
		singleIter: singleIter,
		multiIter:  multiIter,
	}
	it.Reset(slicesIter)
	return it
}

func (it *mixedReadersIterator) Next() bool {
	if it.err != nil || it.closed {
		return false
	}
	switch it.state {
	case activeIterStateNone:
		next := it.slicesIter.Next()
		if !next {
			return false
		}

		readers := it.slicesIter.Current()
		if len(readers) == 1 {
			it.state = activeIterStateSingle
			it.singleIter.Reset(readers[0])
		} else {
			it.state = activeIterStateMulti
			it.multiIter.Reset(readers)
		}

		// Try selected iterator
		return it.Next()
	case activeIterStateSingle:
		next := it.singleIter.Next()
		if it.err == nil {
			it.err = it.singleIter.Err()
		}
		if next {
			return true
		}

		// Try next slice
		it.state = activeIterStateNone
		return it.Next()
	case activeIterStateMulti:
		next := it.multiIter.Next()
		if it.err == nil {
			it.err = it.multiIter.Err()
		}
		if next {
			return true
		}

		// Try next slice
		it.state = activeIterStateNone
		return it.Next()
	}
	return false
}

func (it *mixedReadersIterator) Current() (m3db.Datapoint, xtime.Unit, m3db.Annotation) {
	if it.err == nil && !it.closed {
		switch it.state {
		case activeIterStateSingle:
			return it.singleIter.Current()
		case activeIterStateMulti:
			return it.multiIter.Current()
		}
	}
	return m3db.Datapoint{}, xtime.Unit(0), nil
}

func (it *mixedReadersIterator) Err() error {
	return it.err
}

func (it *mixedReadersIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	// TODO(r): enable pooling and return to pool
}

func (it *mixedReadersIterator) Reset(slicesIter m3db.ReaderSliceOfSlicesIterator) {
	it.slicesIter = slicesIter
	it.state = activeIterStateNone
	it.err = nil
	it.closed = false
}
