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

package ts

import (
	"time"

	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

type writeBatch struct {
	// The maximum batch size that will be allowed to be retained in a call to
	// Reset().
	maxBatchSize int
	writes       []BatchWrite
	ns           ident.ID
	finalizeFn   func(WriteBatch)
}

// NewWriteBatch creates a new WriteBatch.
func NewWriteBatch(
	batchSize int,
	maxBatchSize int,
	ns ident.ID,
	finalizeFn func(WriteBatch),
) WriteBatch {
	return &writeBatch{
		maxBatchSize: maxBatchSize,
		writes:       make([]BatchWrite, 0, batchSize),
		ns:           ns,
		finalizeFn:   finalizeFn,
	}
}

func (b *writeBatch) Add(
	originalIndex int,
	id ident.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) {
	write := newBatchWriterWrite(
		originalIndex, b.ns, id, nil, timestamp, value, unit, annotation)
	b.writes = append(b.writes, write)
}

func (b *writeBatch) AddTagged(
	originalIndex int,
	id ident.ID,
	tagIter ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) {
	write := newBatchWriterWrite(
		originalIndex, b.ns, id, tagIter, timestamp, value, unit, annotation)
	b.writes = append(b.writes, write)
}

func (b *writeBatch) Reset(
	batchSize int,
	ns ident.ID,
	shardFn sharding.HashFn,
) {
	var writes []BatchWrite
	if batchSize > cap(b.writes) || cap(b.writes) > b.maxBatchSize {
		writes = make([]BatchWrite, 0, batchSize)
	} else {
		writes = b.writes[:0]
	}

	b.writes = writes
	b.ns = ns
}

func (b *writeBatch) Iter() []BatchWrite {
	return b.writes
}

func (b *writeBatch) SetSeries(idx int, series Series) {
	b.writes[idx].Write.Series = series
}

func (b *writeBatch) Finalize() {
	b.finalizeFn(b)
}

func newBatchWriterWrite(
	originalIndex int,
	namespace ident.ID,
	id ident.ID,
	tagsIter ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) BatchWrite {
	return BatchWrite{
		Write: Write{
			Series: Series{
				ID:        id,
				Namespace: namespace,
			},
			Datapoint: Datapoint{
				Timestamp: timestamp,
				Value:     value,
			},
			Unit:       unit,
			Annotation: annotation,
		},
		TagIter:       tagsIter,
		OriginalIndex: originalIndex,
	}
}
