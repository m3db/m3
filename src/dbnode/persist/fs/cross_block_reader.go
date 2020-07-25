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
)

var (
	errReaderNotOrderedByIndex = errors.New("CrossBlockReader can only use DataFileSetReaders ordered by index")
	_ heap.Interface   = (*minHeap)(nil)
)

type crossBlockReader struct {
	dataFileSetReaders []DataFileSetReader
	initialized        bool
	minHeap            minHeap
	err                error
}

// NewCrossBlockReader constructs a new CrossBlockReader based on given DataFileSetReaders.
// DataFileSetReaders must be configured to return the data in the order of index, and must be
// provided in a slice sorted by block start time.
// Callers are responsible for closing the DataFileSetReaders.
func NewCrossBlockReader(dataFileSetReaders []DataFileSetReader) (CrossBlockReader, error) {
	var previousStart time.Time
	for _, dataFileSetReader := range dataFileSetReaders {
		if !dataFileSetReader.IsOrderedByIndex() {
			return nil, errReaderNotOrderedByIndex
		}
		currentStart := dataFileSetReader.Range().Start
		if !currentStart.After(previousStart) {
			return nil, fmt.Errorf("dataFileSetReaders are not ordered by time (%s followed by %s)", previousStart, currentStart)
		}
		previousStart = currentStart
	}

	return &crossBlockReader{dataFileSetReaders: dataFileSetReaders}, nil
}

func (r *crossBlockReader) Read() (id ident.ID, tags ident.TagIterator, data checked.Bytes, checksum uint32, err error) {
	if !r.initialized {
		r.initialized = true
		err := r.init()
		if err != nil {
			return nil, nil, nil, 0, err
		}
	}

	if len(r.minHeap) == 0 {
		return nil, nil, nil, 0, io.EOF
	}

	entry := heap.Pop(&r.minHeap).(*minHeapEntry)
	if r.dataFileSetReaders[entry.dataFileSetReaderIndex] != nil {
		nextEntry, err := r.readFromDataFileSet(entry.dataFileSetReaderIndex)
		if err == io.EOF {
			// will no longer read from this one
			r.dataFileSetReaders[entry.dataFileSetReaderIndex] = nil
		} else if err != nil {
			return nil, nil, nil, 0, err
		} else {
			heap.Push(&r.minHeap, nextEntry)
		}
	}

	return entry.id, entry.tags, entry.data, entry.checksum, nil
}

func (r *crossBlockReader) init() error {
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

	return &minHeapEntry{
		dataFileSetReaderIndex: index,
		id:                     id,
		tags:                   tags,
		data:                   data,
		checksum:               checksum,
	}, nil
}

func (r *crossBlockReader) Close() error {
	for _, entry := range r.minHeap {
		entry.id.Finalize()
		entry.tags.Close()
		entry.data.DecRef()
		entry.data.Finalize()
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
	*h = old[0 : n-1]
	return x
}
