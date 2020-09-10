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
	"container/heap"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3/src/x/checked"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

var (
	errReaderNotOrderedByIndex                = errors.New("crossBlockReader can only use DataFileSetReaders ordered by index")
	errEmptyReader                            = errors.New("trying to read from empty reader")
	_                          heap.Interface = (*minHeap)(nil)
)

type crossBlockReader struct {
	dataFileSetReaders []DataFileSetReader
	id                 ident.ID
	tags               ident.TagIterator
	records            []BlockRecord
	started            bool
	minHeap            minHeap
	err                error
	iOpts              instrument.Options
}

// NewCrossBlockReader constructs a new CrossBlockReader based on given DataFileSetReaders.
// DataFileSetReaders must be configured to return the data in the order of index, and must be
// provided in a slice sorted by block start time.
// Callers are responsible for closing the DataFileSetReaders.
func NewCrossBlockReader(dataFileSetReaders []DataFileSetReader, iOpts instrument.Options) (CrossBlockReader, error) {
	var previousStart time.Time
	for _, dataFileSetReader := range dataFileSetReaders {
		if !dataFileSetReader.OrderedByIndex() {
			return nil, errReaderNotOrderedByIndex
		}
		currentStart := dataFileSetReader.Range().Start
		if !currentStart.After(previousStart) {
			return nil, errors.New("dataFileSetReaders are not ordered by time")
		}
		previousStart = currentStart
	}

	return &crossBlockReader{
		dataFileSetReaders: append(make([]DataFileSetReader, 0, len(dataFileSetReaders)), dataFileSetReaders...),
		records:            make([]BlockRecord, 0, len(dataFileSetReaders)),
		iOpts:              iOpts,
	}, nil
}

func (r *crossBlockReader) Next() bool {
	if r.err != nil {
		return false
	}

	var emptyRecord BlockRecord
	if !r.started {
		if r.err = r.start(); r.err != nil {
			return false
		}
	} else {
		// use empty var in inner loop with "for i := range" to have compiler use memclr optimization
		// see: https://codereview.appspot.com/137880043
		for i := range r.records {
			r.records[i] = emptyRecord
		}
	}

	if len(r.minHeap) == 0 {
		return false
	}

	firstEntry, err := r.readOne()
	if err != nil {
		r.err = err
		return false
	}

	r.id = firstEntry.id
	r.tags = firstEntry.tags

	r.records = r.records[:0]
	r.records = append(r.records, BlockRecord{firstEntry.data, firstEntry.checksum})

	// as long as id stays the same across the blocks, accumulate records for this id/tags
	for len(r.minHeap) > 0 && bytes.Equal(r.minHeap[0].id.Bytes(), firstEntry.id.Bytes()) {
		nextEntry, err := r.readOne()
		if err != nil {
			// Close the resources that were already read but not returned to the consumer.
			r.id.Finalize()
			r.tags.Close()
			for _, record := range r.records {
				record.Data.Finalize()
			}
			for i := range r.records {
				r.records[i] = emptyRecord
			}
			r.records = r.records[:0]
			r.err = err
			return false
		}

		// id and tags not needed for subsequent blocks because they are the same as in the first block
		nextEntry.id.Finalize()
		nextEntry.tags.Close()

		r.records = append(r.records, BlockRecord{nextEntry.data, nextEntry.checksum})
	}

	return true
}

func (r *crossBlockReader) Current() (ident.ID, ident.TagIterator, []BlockRecord) {
	return r.id, r.tags, r.records
}

func (r *crossBlockReader) readOne() (*minHeapEntry, error) {
	if len(r.minHeap) == 0 {
		return nil, errEmptyReader
	}

	entry := heap.Pop(&r.minHeap).(*minHeapEntry)
	entry.data.DecRef()

	if r.dataFileSetReaders[entry.dataFileSetReaderIndex] != nil {
		nextEntry, err := r.readFromDataFileSet(entry.dataFileSetReaderIndex)
		if err == io.EOF {
			// will no longer read from this one
			r.dataFileSetReaders[entry.dataFileSetReaderIndex] = nil
		} else if err != nil {
			entry.id.Finalize()
			entry.tags.Close()
			entry.data.Finalize()

			return nil, err

		} else if bytes.Equal(nextEntry.id.Bytes(), entry.id.Bytes()) {
			err := fmt.Errorf("duplicate id %s on block starting at %s",
				entry.id, r.dataFileSetReaders[entry.dataFileSetReaderIndex].Range().Start)

			entry.id.Finalize()
			entry.tags.Close()
			entry.data.Finalize()

			nextEntry.id.Finalize()
			nextEntry.tags.Close()
			nextEntry.data.DecRef()
			nextEntry.data.Finalize()

			instrument.EmitAndLogInvariantViolation(r.iOpts, func(l *zap.Logger) {
				l.Error(err.Error())
			})

			return nil, err

		} else {
			heap.Push(&r.minHeap, nextEntry)
		}
	}

	return entry, nil
}

func (r *crossBlockReader) start() error {
	r.started = true
	r.minHeap = make([]*minHeapEntry, 0, len(r.dataFileSetReaders))

	for i := range r.dataFileSetReaders {
		entry, err := r.readFromDataFileSet(i)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		r.minHeap = append(r.minHeap, entry)
	}

	heap.Init(&r.minHeap)

	return nil
}

func (r *crossBlockReader) readFromDataFileSet(index int) (*minHeapEntry, error) {
	id, tags, data, checksum, err := r.dataFileSetReaders[index].Read()

	if err == io.EOF {
		return nil, err
	}

	if err != nil {
		multiErr := xerrors.NewMultiError().
			Add(err).
			Add(r.Close())
		return nil, multiErr.FinalError()
	}

	data.IncRef()

	return &minHeapEntry{
		dataFileSetReaderIndex: index,
		id:                     id,
		tags:                   tags,
		data:                   data,
		checksum:               checksum,
	}, nil
}

func (r *crossBlockReader) Err() error {
	return r.err
}

func (r *crossBlockReader) Close() error {
	// Close the resources that were buffered in minHeap.
	for i, entry := range r.minHeap {
		entry.id.Finalize()
		entry.tags.Close()

		entry.data.DecRef()
		entry.data.Finalize()
		r.minHeap[i] = nil
	}

	r.minHeap = r.minHeap[:0]
	return nil
}

type minHeapEntry struct {
	dataFileSetReaderIndex int
	id                     ident.ID
	tags                   ident.TagIterator
	data                   checked.Bytes
	checksum               uint32
}

type minHeap []*minHeapEntry

func (h minHeap) Len() int {
	return len(h)
}

func (h minHeap) Less(i, j int) bool {
	idsCmp := bytes.Compare(h[i].id.Bytes(), h[j].id.Bytes())
	if idsCmp == 0 {
		return h[i].dataFileSetReaderIndex < h[j].dataFileSetReaderIndex
	}
	return idsCmp < 0
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(*minHeapEntry))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}
