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

package writes

import (
	"errors"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errTagsAndEncodedTagsRequired = errors.New("tags iterator and encoded tags must be provided")
)

const (
	// preallocateBatchCoeff is used for allocating write batches of slightly bigger
	// capacity than needed for the current request, in order to reduce allocations on
	// subsequent reuse of pooled write batch.
	preallocateBatchCoeff = 1.2
)

type writeBatch struct {
	writes       []BatchWrite
	pendingIndex []PendingIndexInsert
	ns           ident.ID
	// Enables callers to pool encoded tags by allowing them to
	// provide a function to finalize all encoded tags once the
	// writeBatch itself gets finalized.
	finalizeEncodedTagsFn FinalizeEncodedTagsFn
	// Enables callers to pool annotations by allowing them to
	// provide a function to finalize all annotations once the
	// writeBatch itself gets finalized.
	finalizeAnnotationFn FinalizeAnnotationFn
	finalizeFn           func(WriteBatch)
}

// NewWriteBatch creates a new WriteBatch.
func NewWriteBatch(
	initialBatchSize int,
	ns ident.ID,
	finalizeFn func(WriteBatch),
) WriteBatch {
	var (
		writes       []BatchWrite
		pendingIndex []PendingIndexInsert
	)

	if initialBatchSize > 0 {
		writes = make([]BatchWrite, 0, initialBatchSize)
		pendingIndex = make([]PendingIndexInsert, 0, initialBatchSize)
		// Leaving nil slices if initialBatchSize == 0,
		// they will be allocated when needed, based on the actual batch size.
	}

	return &writeBatch{
		writes:       writes,
		pendingIndex: pendingIndex,
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
) error {
	write, err := newBatchWriterWrite(
		originalIndex, b.ns, id, nil, nil, timestamp, value, unit, annotation)
	if err != nil {
		return err
	}
	b.writes = append(b.writes, write)
	return nil
}

func (b *writeBatch) AddTagged(
	originalIndex int,
	id ident.ID,
	tagIter ident.TagIterator,
	encodedTags ts.EncodedTags,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	write, err := newBatchWriterWrite(
		originalIndex, b.ns, id, tagIter, encodedTags, timestamp, value, unit, annotation)
	if err != nil {
		return err
	}
	b.writes = append(b.writes, write)
	return nil
}

func (b *writeBatch) Reset(
	batchSize int,
	ns ident.ID,
) {
	// Preallocate slightly more when not using initialBatchSize.
	preallocateBatchCap := int(float32(batchSize) * preallocateBatchCoeff)

	if batchSize > cap(b.writes) {
		batchCap := batchSize
		if cap(b.writes) == 0 {
			batchCap = preallocateBatchCap
		}
		b.writes = make([]BatchWrite, 0, batchCap)
	} else {
		b.writes = b.writes[:0]
	}

	if batchSize > cap(b.pendingIndex) {
		batchCap := batchSize
		if cap(b.pendingIndex) == 0 {
			batchCap = preallocateBatchCap
		}
		b.pendingIndex = make([]PendingIndexInsert, 0, batchCap)
	} else {
		b.pendingIndex = b.pendingIndex[:0]
	}

	b.ns = ns
	b.finalizeEncodedTagsFn = nil
	b.finalizeAnnotationFn = nil
}

func (b *writeBatch) Iter() []BatchWrite {
	return b.writes
}

func (b *writeBatch) SetSeries(idx int, series ts.Series) {
	b.writes[idx].SkipWrite = false
	b.writes[idx].Write.Series = series
	// Make sure that the EncodedTags does not get clobbered
	b.writes[idx].Write.Series.EncodedTags = b.writes[idx].EncodedTags
}

func (b *writeBatch) SetError(idx int, err error) {
	b.writes[idx].SkipWrite = true
	b.writes[idx].Err = err
}

func (b *writeBatch) SetSkipWrite(idx int) {
	b.writes[idx].SkipWrite = true
}

func (b *writeBatch) SetPendingIndex(idx int, pending PendingIndexInsert) {
	b.writes[idx].PendingIndex = true
	b.pendingIndex = append(b.pendingIndex, pending)
}

func (b *writeBatch) PendingIndex() []PendingIndexInsert {
	return b.pendingIndex
}

// SetFinalizeEncodedTagsFn sets the function that will be called to finalize encodedTags
// when a WriteBatch is finalized, allowing the caller to pool them.
func (b *writeBatch) SetFinalizeEncodedTagsFn(f FinalizeEncodedTagsFn) {
	b.finalizeEncodedTagsFn = f
}

// SetFinalizeAnnotationFn sets the function that will be called to finalize annotations
// when a WriteBatch is finalized, allowing the caller to pool them.
func (b *writeBatch) SetFinalizeAnnotationFn(f FinalizeAnnotationFn) {
	b.finalizeAnnotationFn = f
}

func (b *writeBatch) Finalize() {
	if b.finalizeEncodedTagsFn != nil {
		for _, write := range b.writes {
			encodedTags := write.EncodedTags
			if encodedTags == nil {
				continue
			}

			b.finalizeEncodedTagsFn(encodedTags)
		}
	}
	b.finalizeEncodedTagsFn = nil

	if b.finalizeAnnotationFn != nil {
		for _, write := range b.writes {
			annotation := write.Write.Annotation
			if annotation == nil {
				continue
			}

			b.finalizeAnnotationFn(annotation)
		}
	}
	b.finalizeAnnotationFn = nil

	b.ns = nil

	var zeroedWrite BatchWrite
	for i := range b.writes {
		// Remove any remaining pointers for G.C reasons.
		b.writes[i] = zeroedWrite
	}
	b.writes = b.writes[:0]

	var zeroedIndex PendingIndexInsert
	for i := range b.pendingIndex {
		// Remove any remaining pointers for G.C reasons.
		b.pendingIndex[i] = zeroedIndex
	}
	b.pendingIndex = b.pendingIndex[:0]

	b.finalizeFn(b)
}

func (b *writeBatch) cap() int {
	return cap(b.writes)
}

func newBatchWriterWrite(
	originalIndex int,
	namespace ident.ID,
	id ident.ID,
	tagIter ident.TagIterator,
	encodedTags ts.EncodedTags,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) (BatchWrite, error) {
	write := tagIter == nil && encodedTags == nil
	writeTagged := tagIter != nil && encodedTags != nil
	if !write && !writeTagged {
		return BatchWrite{}, errTagsAndEncodedTagsRequired
	}
	return BatchWrite{
		Write: Write{
			Series: ts.Series{
				ID:          id,
				EncodedTags: encodedTags,
				Namespace:   namespace,
			},
			Datapoint: ts.Datapoint{
				Timestamp:      timestamp,
				TimestampNanos: xtime.ToUnixNano(timestamp),
				Value:          value,
			},
			Unit:       unit,
			Annotation: annotation,
		},
		TagIter:       tagIter,
		EncodedTags:   encodedTags,
		OriginalIndex: originalIndex,
	}, nil
}
