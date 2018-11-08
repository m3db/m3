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

type batchWriter struct {
	// The maximum batch size that will be allowed to be retained in a call to
	// Reset().
	maxBatchSize int
	writes       []BatchWriterWrite
	ns           ident.ID
	shardFn      sharding.HashFn
}

type writeBatchIter struct {
	writes  []BatchWriterWrite
	iterPos int
}

func NewBatchWriter(batchSize int, ns ident.ID, shardFn sharding.HashFn) *batchWriter {
	return &batchWriter{
		// TODO: Pool me
		writes:  make([]BatchWriterWrite, 0, batchSize),
		ns:      ns,
		shardFn: shardFn,
	}
}

func (b *batchWriter) Add(
	id ident.ID,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) {
	write := newBatchWriterWrite(
		b.ns, id, nil, timestamp, value, unit, annotation)
	write.Write.Series.Shard = b.shardFn(id)
	b.writes = append(b.writes, write)
}

func (b *batchWriter) AddTagged(
	id ident.ID,
	tagIter ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) {
	write := newBatchWriterWrite(
		b.ns, id, tagIter, timestamp, value, unit, annotation)
	write.Write.Series.Shard = b.shardFn(id)
	b.writes = append(b.writes, write)
}

func (b *batchWriter) Reset(
	batchSize int,
	ns ident.ID,
	shardFn sharding.HashFn,
) {
	var writes []BatchWriterWrite
	if batchSize > cap(b.writes) || cap(b.writes) > b.maxBatchSize {
		writes = make([]BatchWriterWrite, 0, batchSize)
	} else {
		writes = b.writes[:0]
	}

	b.writes = writes
	b.ns = ns
	b.shardFn = shardFn
}

func (b *batchWriter) Iter() WriteBatchIter {
	return &writeBatchIter{
		writes:  b.writes,
		iterPos: 0,
	}
}

func (i *writeBatchIter) Current() BatchWriterWrite {
	return i.writes[i.iterPos]
}

func (i *writeBatchIter) Next() bool {
	if i.iterPos < len(i.writes)-1 {
		i.iterPos++
		return true
	}

	return false
}

func (i *writeBatchIter) UpdateSeries(s Series) {
	i.writes[i.iterPos].Write.Series = s
}

type BatchWriterWrite struct {
	Write   Write
	TagIter ident.TagIterator
}

func newBatchWriterWrite(
	namespace ident.ID,
	id ident.ID,
	tagsIter ident.TagIterator,
	timestamp time.Time,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) BatchWriterWrite {
	return BatchWriterWrite{
		Write: Write{
			Series: Series{
				ID: id,
			},
			Datapoint: Datapoint{
				Timestamp: timestamp,
				Value:     value,
			},
			Unit:       unit,
			Annotation: annotation,
		},
		TagIter: tagsIter,
	}
}

type BatchWriter interface {
	// stoes an AppenderWrite internalally for each of these
	Add(
		namespace ident.ID,
		id ident.ID,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	)

	AddTagged(
		namespace ident.ID,
		id ident.ID,
		tags ident.TagIterator,
		timestamp time.Time,
		value float64,
		unit xtime.Unit,
		annotation []byte,
	)
}

// Not Exposed to users
type WriteBatchIface interface {
	Iter() WriteBatchIter
	Reset(
		batchSize int,
		ns ident.ID,
		shardFn sharding.HashFn,
	)
}

type WriteBatchIter interface {
	Current() BatchWriterWrite
	UpdateSeries(Series)
	Next() bool
}

// WriteBatch is a batch of Writes.
type WriteBatch struct {
	Writes []Write
}
