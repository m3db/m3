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
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type data struct {
	dp         ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
}

type idxData struct {
	data
	isBase bool
	idx    int
}

func buildBaseIdxData(it encoding.SeriesIterator) idxData {
	dp, unit, annotation := it.Current()
	return idxData{
		isBase: true,
		data: data{
			dp:         dp,
			unit:       unit,
			annotation: annotation,
		},
	}
}

func buildIndexIdxData(it encoding.SeriesIterator, idx int) idxData {
	dp, unit, annotation := it.Current()
	return idxData{
		idx: idx,
		data: data{
			dp:         dp,
			unit:       unit,
			annotation: annotation,
		},
	}
}

type idxDatas []idxData

func (d idxDatas) insert(insertData idxData) {
	addingTime := insertData.data.dp.Timestamp
	idx := len(d)
	for i, data := range d {
		existingTime := data.dp.Timestamp
		if existingTime.Before(addingTime) {
			continue
		}

		if existingTime.Equal(addingTime) {
			if data.isBase || data.idx < insertData.idx {
				continue
			}
		}

		idx = i
		break
	}

	d = append(d, idxData{})
	copy(d[:idx+1], d[:idx])
	d[idx] = insertData

	d = append(d[:idx], insertData)
	d = append(d, d[:idx]...)
}

// NB: enforce multiIterator implements encoding.SeriesIterator.
var _ encoding.SeriesIterator = (*multiIterator)(nil)

type multiIterator struct {
	err error

	current   data
	indexData idxDatas
	iters     []encoding.SeriesIterator
	base      encoding.SeriesIterator
}

func (it *multiIterator) Next() bool {
	if len(it.indexData) == 0 {
		return false
	}

	currTs := it.current.dp.Timestamp
	for {
		if len(it.indexData) == 0 {
			return false
		}

		// NB: take off value corresponding to next ordered iterator.
		first := it.indexData[0]
		copy(it.indexData, it.indexData[1:])
		it.indexData = it.indexData[:len(it.indexData)-2]

		var currIdxData idxData
		if first.isBase {
			// NB: this iterator is exhausted. No need to re-add it.
			if !it.base.Next() {
				continue
			}

			currIdxData = buildBaseIdxData(it.base)
		} else {
			iterAtIndex := it.iters[first.idx]
			// NB: this iterator is exhausted. No need to re-add it.
			if !iterAtIndex.Next() {
				continue
			}

			currIdxData = buildIndexIdxData(iterAtIndex, first.idx)
		}

		// Add the next point in the read iterator.
		it.indexData.insert(currIdxData)

		// If the next point's timestamp is after current point, update
		// internal current and return true. Otherwise, repeat for next points.
		ts := first.dp.Timestamp
		if ts.IsZero() || ts.After(currTs) {
			it.current.dp = first.dp
			it.current.unit = first.unit
			it.current.annotation = first.annotation
			return true
		}
	}
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

func newMultiReaderIterator(base encoding.SeriesIterator) *multiIterator {
	return &multiIterator{
		base:      base,
		indexData: idxDatas{buildBaseIdxData(base)},
	}
}

func (it *multiIterator) addSeriesIterator(iter encoding.SeriesIterator) error {
	if !it.base.Namespace().Equal(iter.Namespace()) {
		return fmt.Errorf("mismatched namespace: %s base, %s incoming",
			it.base.Namespace().String(), iter.Namespace().String())
	}

	it.iters = append(it.iters, iter)
	idx := len(it.indexData)
	it.indexData = append(it.indexData, buildIndexIdxData(iter, idx))
	return nil
}

func (it *multiIterator) Reset(opts encoding.SeriesIteratorOptions) {
	// Since this overwrites data, reset only the base and release other iters.
	for _, iter := range it.iters {
		iter.Close()
	}

	it.iters = it.iters[:0]
	it.indexData = it.indexData[:0]
	it.base.Reset(opts)
	// Populate index data list.
	if it.base.Next() {
		currIdxData := buildBaseIdxData(it.base)
		it.indexData = append(it.indexData, currIdxData)
	}
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
