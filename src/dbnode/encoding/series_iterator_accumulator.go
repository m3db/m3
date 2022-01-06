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

package encoding

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

var _ SeriesIteratorAccumulator = (*seriesIteratorAccumulator)(nil)

type seriesIteratorAccumulator struct {
	id              ident.ID
	nsID            ident.ID
	start           xtime.UnixNano
	end             xtime.UnixNano
	iters           iterators
	seriesIterators []SeriesIterator
	err             error
	firstNext       bool
	closed          bool
}

// NewSeriesIteratorAccumulator creates a new series iterator.
func NewSeriesIteratorAccumulator(iter SeriesIterator) (SeriesIteratorAccumulator, error) {
	nsID := ""
	if iter.Namespace() != nil {
		nsID = iter.Namespace().String()
	}
	it := &seriesIteratorAccumulator{
		// NB: clone id and nsID so that they will be accessible after underlying
		// iterators are closed.
		id:              ident.StringID(iter.ID().String()),
		nsID:            ident.StringID(nsID),
		seriesIterators: make([]SeriesIterator, 0, 2),
		firstNext:       true,
	}

	it.iters.reset()

	err := it.Add(iter)
	if err != nil {
		return nil, err
	}

	return it, nil
}

func (it *seriesIteratorAccumulator) Add(iter SeriesIterator) error {
	if it.err != nil {
		return it.err
	}

	if !iter.Next() || !it.iters.push(iter) {
		iter.Close()
		return iter.Err()
	}

	iterStart := iter.Start()
	if start := it.start; start.IsZero() || iterStart.Before(start) {
		it.start = iterStart
	}

	iterEnd := iter.End()
	if end := it.end; end.IsZero() || iterEnd.After(end) {
		it.end = iterEnd
	}

	it.seriesIterators = append(it.seriesIterators, iter)
	return nil
}

func (it *seriesIteratorAccumulator) ID() ident.ID {
	return it.id
}

func (it *seriesIteratorAccumulator) Namespace() ident.ID {
	return it.nsID
}

func (it *seriesIteratorAccumulator) Tags() ident.TagIterator {
	// NB: the tags for each iterator must be the same, so it's valid to return
	// from whichever iterator is available.
	for _, iter := range it.seriesIterators {
		if tags := iter.Tags(); tags != nil {
			return tags
		}
	}

	return ident.EmptyTagIterator
}

func (it *seriesIteratorAccumulator) Start() xtime.UnixNano {
	return it.start
}

func (it *seriesIteratorAccumulator) End() xtime.UnixNano {
	return it.end
}

func (it *seriesIteratorAccumulator) Next() bool {
	if !it.firstNext {
		if !it.hasNext() {
			return false
		}

		it.moveToNext()
	}

	it.firstNext = false
	return it.hasNext()
}

func (it *seriesIteratorAccumulator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return it.iters.current()
}

func (it *seriesIteratorAccumulator) Err() error {
	if it.err != nil {
		return it.err
	}

	for _, iter := range it.seriesIterators {
		if err := iter.Err(); err != nil {
			it.err = err
			return err
		}
	}

	return nil
}

func (it *seriesIteratorAccumulator) FirstAnnotation() ts.Annotation {
	return it.iters.firstAnnotation()
}

func (it *seriesIteratorAccumulator) Close() {
	if it.isClosed() {
		return
	}
	it.closed = true
	if it.id != nil {
		it.id.Finalize()
		it.id = nil
	}
	if it.nsID != nil {
		it.nsID.Finalize()
		it.nsID = nil
	}
	it.iters.reset()
	it.firstNext = true
}

func (it *seriesIteratorAccumulator) Replicas() ([]MultiReaderIterator, error) {
	if l := len(it.seriesIterators); l != 1 {
		return nil, fmt.Errorf("cannot get replicas for accumulated series "+
			"iterators: need 1 iterator, have %d", l)
	}
	return it.seriesIterators[0].Replicas()
}

func (it *seriesIteratorAccumulator) Reset(SeriesIteratorOptions) {
	if it.err == nil {
		it.err = errors.New("cannot reset a series accumulator")
	}
	return
}

func (it *seriesIteratorAccumulator) SetIterateEqualTimestampStrategy(
	strategy IterateEqualTimestampStrategy,
) {
	it.iters.equalTimesStrategy = strategy
	for _, iter := range it.seriesIterators {
		iter.SetIterateEqualTimestampStrategy(strategy)
	}
}

func (it *seriesIteratorAccumulator) hasError() bool {
	return it.err != nil
}

func (it *seriesIteratorAccumulator) isClosed() bool {
	return it.closed
}

func (it *seriesIteratorAccumulator) hasMore() bool {
	return it.iters.len() > 0
}

func (it *seriesIteratorAccumulator) hasNext() bool {
	return !it.hasError() && !it.isClosed() && it.hasMore()
}

func (it *seriesIteratorAccumulator) moveToNext() {
	for {
		prev := it.iters.at()
		next, err := it.iters.moveToValidNext()
		if err != nil {
			it.err = err
			return
		}
		if !next {
			return
		}

		curr := it.iters.at()
		if curr != prev {
			return
		}

		// Dedupe by continuing
	}
}

func (it *seriesIteratorAccumulator) Stats() (SeriesIteratorStats, error) {
	approx := 0
	for _, iter := range it.seriesIterators {
		stats, err := iter.Stats()
		if err != nil {
			return SeriesIteratorStats{}, err
		}
		approx += stats.ApproximateSizeInBytes
	}
	return SeriesIteratorStats{ApproximateSizeInBytes: approx}, nil
}
