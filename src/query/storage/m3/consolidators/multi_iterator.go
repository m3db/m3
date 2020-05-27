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

package consolidators

import (
	"errors"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type data struct {
	hasValue   bool
	dp         ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

type idxData struct {
	data
	isBase bool
	idx  int
}

// NB: enforce multiIterator implements encoding.SeriesIterator.
var _ encoding.SeriesIterator = (*multiIterator)(nil)

type multiIterator struct {
	err error

	current    data
	currentIdx int
	indexData  []idxData
	iters      []encoding.SeriesIterator
	base       encoding.SeriesIterator
}

func (it *multiIterator) moveNext() bool {
	if len(it.indexData) == 0 {
		return false
	}

	// NB: if the next value to read is from base, increment base iterator.
	next := it.indexData[0]
	if next.isBase {
		if it.base.Next() {
			
		}
	}
}

func (it *multiIterator) Next() bool {
	if !it.current.hasValue {

	}

	for idx, currVal := range it.allVals {
		if currVal.hasValue != nil {
			return false
		}
	}

	return true
}

func (it *multiIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return it.current.dp, it.current.unit, it.current.annotation
}

func (it *multiIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(it.base.Err())
	for _, iter := range it.iters {
		multiErr = multiErr.Add(iter.Err())
	}

	it.err = multiErr.FinalError()
	return it.err
}

func (it *multiIterator) Close() {
	it.base.Close()
	for _, iter := range it.iters {
		iter.Close()
	}
}

func (it *multiIterator) ID() ident.ID {
	return it.base.ID()
}

func (it *multiIterator) Namespace() ident.ID {
	return it.base.Namespace()
}

func (it *multiIterator) Tags() ident.TagIterator {
	return it.base.Tags()
}

func (it *multiIterator) Start() time.Time {
	start := it.base.Start()
	for _, iter := range it.iters {
		if other := iter.Start(); other.Before(start) {
			start = other
		}
	}

	return start
}

func (it *multiIterator) End() time.Time {
	end := it.base.End()
	for _, iter := range it.iters {
		if other := iter.End(); other.After(end) {
			end = other
		}
	}

	return end
}

func (it *multiIterator) Reset(opts encoding.SeriesIteratorOptions) {
	// Since this overwrites data, reset only the base and release other iters.
	for _, iter := range it.iters {
		iter.Close()
	}

	it.iters = it.iters[:0]
	it.current.hasValue = false
	it.base.Reset(opts)
}

func (it *multiIterator) SetIterateEqualTimestampStrategy(
	strategy encoding.IterateEqualTimestampStrategy) {
	it.base.SetIterateEqualTimestampStrategy(strategy)
	for _, iter := range it.iters {
		iter.SetIterateEqualTimestampStrategy(strategy)
	}
}

func (it *multiIterator) Replicas() []encoding.MultiReaderIterator {
	it.err = errors.New("cannot receive replicas on a multi-iterator")
	return []encoding.MultiReaderIterator{}
}

func (it *multiIterator) Stats() (encoding.SeriesIteratorStats, error) {
	stats, err := it.base.Stats()
	if err != nil {
		return stats, err
	}

	for _, iter := range it.iters {
		s, err := iter.Stats()
		if err != nil {
			return stats, err
		}

		stats.ApproximateSizeInBytes += s.ApproximateSizeInBytes
	}

	return stats, nil
}
