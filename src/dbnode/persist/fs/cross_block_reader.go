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
	"io"
	"time"

	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errReaderNotOrderedByIndex     = errors.New("crossBlockReader can only use DataFileSetReaders ordered by index")
	errUnorderedDataFileSetReaders = errors.New("dataFileSetReaders are not ordered by time")

	_ heap.Interface = (*minHeap)(nil)
)

type crossBlockReader struct {
	dataFileSetReaders   []DataFileSetReader
	pendingReaderIndices []int
	minHeap              minHeap

	iOpts instrument.Options

	id          ident.BytesID
	encodedTags []byte
	records     []BlockRecord
	err         error
}

// NewCrossBlockReader constructs a new CrossBlockReader based on given DataFileSetReaders.
// DataFileSetReaders must be configured to return the data in the order of index, and must be
// provided in a slice sorted by block start time.
// Callers are responsible for closing the DataFileSetReaders.
func NewCrossBlockReader(dataFileSetReaders []DataFileSetReader, iOpts instrument.Options) (CrossBlockReader, error) {
	var previousStart time.Time
	for _, dataFileSetReader := range dataFileSetReaders {
		if !dataFileSetReader.StreamingEnabled() {
			return nil, errReaderNotOrderedByIndex
		}
		currentStart := dataFileSetReader.Range().Start
		if !currentStart.After(previousStart) {
			return nil, errUnorderedDataFileSetReaders
		}
		previousStart = currentStart
	}

	pendingReaderIndices := make([]int, len(dataFileSetReaders))
	for i := range dataFileSetReaders {
		pendingReaderIndices[i] = i
	}

	return &crossBlockReader{
		dataFileSetReaders:   dataFileSetReaders,
		pendingReaderIndices: pendingReaderIndices,
		minHeap:              make([]minHeapEntry, 0, len(dataFileSetReaders)),
		records:              make([]BlockRecord, 0, len(dataFileSetReaders)),
		iOpts:                iOpts,
	}, nil
}

func (r *crossBlockReader) Next() bool {
	if r.err != nil {
		return false
	}

	// use empty var in inner loop with "for i := range" to have compiler use memclr optimization
	// see: https://codereview.appspot.com/137880043
	var emptyRecord BlockRecord
	for i := range r.records {
		r.records[i] = emptyRecord
	}
	r.records = r.records[:0]

	if len(r.pendingReaderIndices) == 0 {
		return false
	}

	for _, readerIndex := range r.pendingReaderIndices {
		entry, err := r.readFromDataFileSet(readerIndex)
		if err == io.EOF {
			// Will no longer read from this one.
			continue
		} else if err != nil {
			r.err = err
			return false
		} else {
			heap.Push(&r.minHeap, entry)
		}
	}

	r.pendingReaderIndices = r.pendingReaderIndices[:0]

	if len(r.minHeap) == 0 {
		return false
	}

	firstEntry := heap.Pop(&r.minHeap).(minHeapEntry)

	r.id = firstEntry.id
	r.encodedTags = firstEntry.encodedTags

	r.records = append(r.records, BlockRecord{firstEntry.data, firstEntry.checksum})

	// We have consumed an entry from this dataFileSetReader, so need to schedule a read from it on the next Next().
	r.pendingReaderIndices = append(r.pendingReaderIndices, firstEntry.dataFileSetReaderIndex)

	// As long as id stays the same across the blocks, accumulate records for this id/tags.
	for len(r.minHeap) > 0 && bytes.Equal(r.minHeap[0].id.Bytes(), firstEntry.id.Bytes()) {
		nextEntry := heap.Pop(&r.minHeap).(minHeapEntry)

		r.records = append(r.records, BlockRecord{nextEntry.data, nextEntry.checksum})

		// We have consumed an entry from this dataFileSetReader, so need to schedule a read from it on the next Next().
		r.pendingReaderIndices = append(r.pendingReaderIndices, nextEntry.dataFileSetReaderIndex)
	}

	return true
}

func (r *crossBlockReader) Current() (ident.BytesID, []byte, []BlockRecord) {
	return r.id, r.encodedTags, r.records
}

func (r *crossBlockReader) readFromDataFileSet(index int) (minHeapEntry, error) {
	id, encodedTags, data, checksum, err := r.dataFileSetReaders[index].StreamingRead()

	if err == io.EOF {
		return minHeapEntry{}, err
	}

	if err != nil {
		multiErr := xerrors.NewMultiError().
			Add(err).
			Add(r.Close())
		return minHeapEntry{}, multiErr.FinalError()
	}

	return minHeapEntry{
		dataFileSetReaderIndex: index,
		id:                     id,
		encodedTags:            encodedTags,
		data:                   data,
		checksum:               checksum,
	}, nil
}

func (r *crossBlockReader) Err() error {
	return r.err
}

func (r *crossBlockReader) Close() error {
	var emptyRecord BlockRecord
	for i := range r.records {
		r.records[i] = emptyRecord
	}
	r.records = r.records[:0]

	var emptyEntry minHeapEntry
	for i := range r.minHeap {
		r.minHeap[i] = emptyEntry
	}
	r.minHeap = r.minHeap[:0]

	return nil
}

type minHeapEntry struct {
	id                     ident.BytesID
	encodedTags            []byte
	data                   []byte
	dataFileSetReaderIndex int
	checksum               uint32
}

type minHeap []minHeapEntry

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
	*h = append(*h, x.(minHeapEntry))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = minHeapEntry{}
	*h = old[0 : n-1]
	return x
}
