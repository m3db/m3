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

package storage

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
)

// WideQueryIterator is a wide query iterator.
type WideQueryIterator interface {
	BlockStart() time.Time
	Shards() []uint32

	Next() bool
	Current() WideQueryShardIterator
	Err() error
	Close()
}

// WideQueryShardIterator is a wide query shard iterator.
type WideQueryShardIterator interface {
	Shard() uint32

	Next() bool
	Current() WideQuerySeriesIterator
	Err() error
	Close()
}

// WideQuerySeriesIterator is a wide query series iterator.
type WideQuerySeriesIterator interface {
	ID() ident.ID
	EncodedTags() ts.EncodedTags
	MetadataChecksum() int64

	Next() bool
	Current() (float64, ts.Annotation)
	Err() error
	Close()
}

var _ WideQueryIterator = (*wideQueryIterator)(nil)

type wideQueryIterator struct {
	blockStart time.Time
	shards     []uint32

	fixedBufferMgr *fixedBufferManager
	iters          chan *wideQueryShardIterator

	writeIter  *wideQueryShardIterator
	writeShard uint32

	readIter *wideQueryShardIterator

	state wideQueryIteratorStateShared
}

type wideQueryIteratorStateShared struct {
	sync.Mutex
	// err is the only thing read/written to from both
	// producer and consumer side.
	err error
}

const (
	shardNotSet = math.MaxUint32
	shardEOF    = shardNotSet - 1
)

func newWideQueryIterator(
	blockStart time.Time,
	shards []uint32,
) *wideQueryIterator {
	return &wideQueryIterator{
		fixedBufferMgr: newFixedBufferManager(newFixedBufferPool()),
		iters:          make(chan *wideQueryShardIterator, len(shards)),
		writeShard:     shardNotSet,
	}
}

func (i *wideQueryIterator) setDoneError(err error) {
	i.state.Lock()
	i.state.err = err
	i.state.Unlock()

	i.setDone()
}

func (i *wideQueryIterator) setDone() {
	i.writeShard = shardEOF
	if i.writeIter != nil {
		// Finalize the last iter for writing.
		i.writeIter.setDone()
		i.writeIter = nil
	}
	close(i.iters)
}

func (i *wideQueryIterator) shardIter(
	shard uint32,
) (*wideQueryShardIterator, error) {
	if i.writeShard == shard {
		return i.writeIter, nil
	}

	// Make sure progressing in shard ascending order.
	if i.writeShard != shardNotSet && shard < i.writeShard {
		if i.writeShard == shardEOF {
			return nil, fmt.Errorf(
				"shard progression already complete: attempted_next=%d",
				shard)
		}
		return nil, fmt.Errorf(
			"shard progression must be ascending: curr=%d, next=%d",
			i.writeShard, shard)
	}

	nextShardIter := newWideQueryShardIterator(shard, i.fixedBufferMgr)

	if i.writeIter != nil {
		// Close the current iter for writing.
		i.writeIter.setDone()
	}
	i.writeIter = nextShardIter
	i.writeShard = shard

	i.iters <- nextShardIter
	return nextShardIter, nil
}

func (i *wideQueryIterator) BlockStart() time.Time {
	return i.blockStart
}

func (i *wideQueryIterator) Shards() []uint32 {
	return i.shards
}

func (i *wideQueryIterator) Next() bool {
	iter, ok := <-i.iters
	if !ok {
		i.readIter = nil
		return false
	}

	i.readIter = iter
	return true
}

func (i *wideQueryIterator) Current() WideQueryShardIterator {
	return i.readIter
}

func (i *wideQueryIterator) Err() error {
	i.state.Lock()
	err := i.state.err
	i.state.Unlock()
	return err
}

func (i *wideQueryIterator) Close() {
}

const shardIterRecordsBuffer = 1024

var _ WideQueryShardIterator = (*wideQueryShardIterator)(nil)

type wideQueryShardIterator struct {
	shard          uint32
	fixedBufferMgr *fixedBufferManager

	records chan wideQueryShardIteratorQueuedRecord

	iter *wideQuerySeriesIterator

	state wideQueryShardIteratorSharedState
}

type wideQueryShardIteratorQueuedRecord struct {
	id                []byte
	borrowID          fixedBufferBorrow
	encodedTags       []byte
	borrowEncodedTags fixedBufferBorrow
	data              []byte
	borrowData        fixedBufferBorrow
	metadataChecksum  int64
}

type wideQueryShardIteratorRecord struct {
	ID               []byte
	EncodedTags      []byte
	MetadataChecksum int64
	Data             []byte
}

type wideQueryShardIteratorSharedState struct {
	sync.Mutex
	// err is the only thing read/written to from both
	// producer and consumer side.
	err error
}

func newWideQueryShardIterator(
	shard uint32,
	fixedBufferMgr *fixedBufferManager,
) *wideQueryShardIterator {
	return &wideQueryShardIterator{
		shard:          shard,
		records:        make(chan wideQueryShardIteratorQueuedRecord, shardIterRecordsBuffer),
		fixedBufferMgr: fixedBufferMgr,
	}
}

func (i *wideQueryShardIterator) setDone() {
	close(i.records)
}

func (i *wideQueryShardIterator) pushRecord(
	r wideQueryShardIteratorRecord,
) {
	// TODO: transactionally copy the ID + tags + anything else in one go
	// otherwise the fixed buffer manager might run out of mem and wait
	// for another buffer to be available but the existing buffer cannot
	// be released since one field here has taken a copy and needs to be
	// returned for entire underlying buffer to be released.
	var qr wideQueryShardIteratorQueuedRecord
	qr.id, qr.borrowID = i.fixedBufferMgr.copy(r.ID)
	qr.encodedTags, qr.borrowEncodedTags = i.fixedBufferMgr.copy(r.EncodedTags)
	qr.data, qr.borrowData = i.fixedBufferMgr.copy(r.Data)
	qr.metadataChecksum = r.MetadataChecksum
	i.records <- qr
}

func (i *wideQueryShardIterator) Shard() uint32 {
	return i.shard
}

func (i *wideQueryShardIterator) Next() bool {
	record, ok := <-i.records
	if !ok {
		i.iter = nil
		return false
	}

	if i.iter == nil {
		i.iter = newWideQuerySeriesIterator()
	}
	i.iter.reset(record)
	return true
}

func (i *wideQueryShardIterator) Current() WideQuerySeriesIterator {
	return i.iter
}

func (i *wideQueryShardIterator) Err() error {
	i.state.Lock()
	err := i.state.err
	i.state.Unlock()
	return err
}

func (i *wideQueryShardIterator) Close() {
}

var _ WideQuerySeriesIterator = (*wideQuerySeriesIterator)(nil)

type wideQuerySeriesIterator struct {
	record      wideQueryShardIteratorQueuedRecord
	reuseableID *ident.ReuseableBytesID
}

func newWideQuerySeriesIterator() *wideQuerySeriesIterator {
	return &wideQuerySeriesIterator{
		reuseableID: ident.NewReuseableBytesID(),
	}
}

func (i *wideQuerySeriesIterator) reset(
	record wideQueryShardIteratorQueuedRecord,
) {
	i.record = record
	i.reuseableID.Reset(i.record.id)
}

func (i *wideQuerySeriesIterator) ID() ident.ID {
	return i.reuseableID
}

func (i *wideQuerySeriesIterator) EncodedTags() ts.EncodedTags {
	return ts.EncodedTags(i.record.encodedTags)
}

func (i *wideQuerySeriesIterator) MetadataChecksum() int64 {
	return i.record.metadataChecksum
}

func (i *wideQuerySeriesIterator) Next() bool {
	return false
}

func (i *wideQuerySeriesIterator) Current() (float64, ts.Annotation) {
	return 0, nil
}

func (i *wideQuerySeriesIterator) Err() error {
	return nil
}

func (i *wideQuerySeriesIterator) Close() {
	// Release the borrows on buffers.
	i.record.borrowID.BufferFinalize()
	i.record.borrowEncodedTags.BufferFinalize()
	i.record = wideQueryShardIteratorQueuedRecord{}
}
