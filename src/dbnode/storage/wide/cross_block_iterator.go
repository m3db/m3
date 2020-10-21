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

package wide

import (
	"bytes"
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

type crossBlockIterator struct {
	exhausted bool
	started   bool
	err       error

	iters                  []QueryShardIterator
	itersExhausted         []bool
	pendingIteratorIndices []int

	currBlockSeriesIter *crossBlockSeriesIterator
	currIters           []QuerySeriesIterator
}

// NewCrossBlockIterator constructs a new CrossBlockReaderIterator based on given DataFileSetReaders.
// DataFileSetReaders must be configured to return the data in the order of index, and must be
// provided in a slice sorted by block start time.
// Callers are responsible for closing the DataFileSetReaders.
func NewCrossBlockIterator(
	iters []QueryShardIterator,
) (CrossBlockIterator, error) {
	var (
		previousStart          time.Time
		exhausted              = make([]bool, len(iters))
		pendingIteratorIndices = make([]int, 0, len(iters))
	)

	for idx, iter := range iters {
		currentStart := iter.BlockStart()
		if !currentStart.After(previousStart) {
			return nil, fmt.Errorf("shard iterators out of order: %v before %v",
				currentStart, previousStart)
		}

		// NB: start every iterator; if it has no values, set it to exhausted.
		if !iter.Next() {
			exhausted[idx] = true

			if err := iter.Err(); err != nil {
				return nil, err
			}
		}

		pendingIteratorIndices = append(pendingIteratorIndices, idx)
		previousStart = currentStart
	}

	return &crossBlockIterator{
		iters:                  iters,
		itersExhausted:         exhausted,
		pendingIteratorIndices: pendingIteratorIndices,
		currBlockSeriesIter:    newCrossBlockReaderIterator(),
		currIters:              make([]QuerySeriesIterator, 0, len(iters)),
	}, nil
}

func (it *crossBlockIterator) Next() bool {
	if it.err != nil || it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
	} else {
		// NB: if no iterators are pending, this iterator is exhausted.
		if len(it.pendingIteratorIndices) == 0 {
			it.exhausted = true
			return false
		}

		for _, iterIdx := range it.pendingIteratorIndices {
			iter := it.iters[iterIdx]
			if !iter.Next() {
				it.itersExhausted[iterIdx] = true

				if err := iter.Err(); err != nil {
					it.err = err
					return false
				}
			}
		}

	}

	it.pendingIteratorIndices = it.pendingIteratorIndices[:0]
	it.currIters = it.currIters[:0]
	var minID []byte
	for idx, itersExhausted := range it.itersExhausted {
		if itersExhausted {
			continue
		}

		id := it.iters[idx].Current().SeriesMetadata().ID
		if len(minID) == 0 || bytes.Compare(minID, id) > 0 {
			minID = id
		}
	}

	// NB: if minID was not updated, this iterator is exhausted.
	if len(minID) == 0 {
		it.exhausted = true
		return false
	}

	for idx, itersExhausted := range it.itersExhausted {
		if itersExhausted {
			continue
		}

		curr := it.iters[idx].Current()
		id := curr.SeriesMetadata().ID
		if !bytes.Equal(id, minID) {
			continue
		}

		it.currIters = append(it.currIters, curr)
		it.pendingIteratorIndices = append(it.pendingIteratorIndices, idx)
	}

	it.currBlockSeriesIter.reset(it.currIters)
	return true
}

func (it *crossBlockIterator) Current() QuerySeriesIterator {
	return it.currBlockSeriesIter
}

func (it *crossBlockIterator) Err() error {
	return it.err
}

func (it *crossBlockIterator) Close() {
	it.currIters = it.currIters[:0]
	it.iters = it.iters[:0]
	it.currBlockSeriesIter.Close()
}

type crossBlockSeriesIterator struct {
	err   error
	idx   int
	meta  SeriesMetadata
	iters []QuerySeriesIterator
}

var _ QuerySeriesIterator = (*crossBlockSeriesIterator)(nil)

func newCrossBlockReaderIterator() *crossBlockSeriesIterator {
	return &crossBlockSeriesIterator{}
}

func (c *crossBlockSeriesIterator) Next() bool {
	if c.err != nil || c.idx >= len(c.iters) {
		return false
	}

	currIter := c.iters[c.idx]
	if currIter.Next() {
		return true
	}

	if err := currIter.Err(); err != nil {
		c.err = err
		return false
	}

	// NB: increment internal reader and try reading next again.
	c.idx++
	return c.Next()
}

func (c *crossBlockSeriesIterator) SeriesMetadata() SeriesMetadata {
	return c.meta
}

func (c *crossBlockSeriesIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return c.iters[c.idx].Current()
}

func (c *crossBlockSeriesIterator) reset(iters []QuerySeriesIterator) {
	c.idx = 0
	c.err = nil
	c.iters = iters
	if len(iters) > 0 {
		c.meta = iters[0].SeriesMetadata()
	}
}

func (c *crossBlockSeriesIterator) Close() {
	// NB: do not close held iterators, as they should be closed by upstreams.
	c.iters = c.iters[:0]
}

func (c *crossBlockSeriesIterator) Err() error {
	return c.err
}
